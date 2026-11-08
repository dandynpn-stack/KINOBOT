"""Regression tests for production workflows, concurrency and upgrades."""
import asyncio
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.methods import AnswerCallbackQuery, SendVideo
from aiogram.types import Message, User, Chat

from bot import App, Guard, run_bot
from core import telegram_id, utf16_length
from storage import Store, SCHEMA
from test_bot import movie, asset

class UpgradeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.db=Store(':memory:')
        self.db.touch(1); self.db.touch(2)
        self.bot=SimpleNamespace(id=123,send_message=AsyncMock(),send_video=AsyncMock(),
            send_document=AsyncMock(),send_photo=AsyncMock(),
            get_chat_member=AsyncMock(return_value=SimpleNamespace(status='member')))
        self.app=App(self.bot,self.db,{1}); self.app.username='test_bot'
        self.mid=self.db.create_with_asset(movie(),asset())

    async def asyncTearDown(self): self.db.close()

    def msg(self,text='',uid=1,**kw):
        return SimpleNamespace(from_user=SimpleNamespace(id=uid),text=text,photo=None,
                               video=None,document=None,**kw)

    async def click(self,data,uid=1,answer=None):
        await self.app.callback(SimpleNamespace(from_user=SimpleNamespace(id=uid),data=data,
                                                answer=answer or AsyncMock()))

    def buttons(self,uid=1):
        actions=self.db.keyboard(uid)['actions']
        return [SimpleNamespace(text=b.text,callback_data=actions[b.text]) for row in self.bot.send_message.call_args.kwargs['reply_markup'].keyboard for b in row]

    async def test_expired_callback_ack_does_not_abort_action(self):
        expired=AsyncMock(side_effect=TelegramBadRequest(
            method=AnswerCallbackQuery(callback_query_id='old'),message='query is too old'))
        await self.click(f'asset:{self.mid}',uid=2,answer=expired)
        self.bot.send_video.assert_awaited_once()

    async def test_wizard_choice_is_bound_to_current_step(self):
        await self.app.admin_action(1,['add'])
        await self.app.message(self.msg('500'))
        await self.app.message(self.msg('Yangi kino'))
        button=next(b for b in self.buttons() if b.text=='🎬 Kino')
        await self.click(button.callback_data)
        self.assertEqual(self.db.state(1)['step'],3)
        skip=next(b for b in self.buttons() if 'O‘tkazib' in b.text)
        await self.click(button.callback_data)
        self.assertEqual(self.db.state(1)['step'],3)
        await self.click(skip.callback_data)
        self.assertEqual(self.db.state(1)['step'],4)
        self.assertEqual(self.db.state(1)['data']['genre'],'')

    async def test_wizard_buttons_recheck_permissions(self):
        self.db.run('INSERT INTO admins VALUES(2,?)',('editor',))
        await self.app.admin_action(2,['add'])
        await self.app.message(self.msg('501',uid=2))
        await self.app.message(self.msg('Film',uid=2))
        button=next(b for b in self.buttons(2) if b.text=='🎬 Kino')
        self.db.run('DELETE FROM admins WHERE user_id=2')
        await self.click(button.callback_data,uid=2)
        self.assertIsNone(self.db.state(2))

    async def test_typed_input_invalidates_old_choice(self):
        await self.app.begin_asset(1,self.mid)
        button=self.buttons()[0]
        await self.app.message(self.msg('Turkcha'))
        before=self.db.state(1)
        await self.click(button.callback_data)
        self.assertEqual(self.db.state(1),before)

    async def test_navigation_clears_admin_draft_and_pending_movie(self):
        await self.app.admin_action(1,['add'])
        self.db.run('UPDATE users SET pending=? WHERE id=1',('open:1',))
        await self.click('list:all:0:0')
        self.assertIsNone(self.db.state(1))
        self.assertIsNone(self.db.one('SELECT pending FROM users WHERE id=1')['pending'])
        await self.app.message(self.msg('101'))
        self.bot.send_video.assert_awaited_once()

    async def test_admin_navigation_discards_previous_workflow(self):
        await self.app.admin_action(1,['broadcast'])
        await self.app.admin_action(1,['manage','0'])
        self.assertIsNone(self.db.state(1))

    async def test_back_to_card_does_not_resend_video(self):
        await self.app.open_code(2,'101')
        await self.click('view:1',uid=2)
        self.bot.send_video.assert_awaited_once()
        self.assertEqual(self.db.one('SELECT COUNT(*) n FROM deliveries')['n'],1)

    async def test_info_view_resumes_after_subscription_without_video(self):
        self.db.run('INSERT INTO channels(chat_id,title,url,required) VALUES(-100,?,?,1)',('Channel','https://t.me/test'))
        self.bot.get_chat_member.return_value=SimpleNamespace(status='left')
        await self.click('view:1',uid=2)
        self.assertEqual(self.db.one('SELECT pending FROM users WHERE id=2')['pending'],'view:1')
        self.bot.get_chat_member.return_value=SimpleNamespace(status='member')
        await self.click('check',uid=2)
        self.bot.send_video.assert_not_awaited()

    async def test_next_episode_crosses_seasons_and_respects_visibility(self):
        mid=self.db.create_with_asset(movie('S1','series'),asset(season=1,episode=2))
        next_id=self.db.add_asset(mid,asset(season=2,episode=1))
        await self.app.send_asset(2,next_id)
        markup=self.bot.send_video.call_args.kwargs['reply_markup']
        callbacks=[self.db.keyboard(2)['actions'][b.text] for row in markup.keyboard for b in row]
        self.assertIn(f'variants:{mid}:1:2:0',callbacks)
        self.db.run('UPDATE movies SET hidden=1 WHERE id=?',(mid,))
        await self.app.send_asset(2,next_id)
        self.bot.send_video.assert_awaited_once()

    async def test_long_unicode_poster_caption_is_split(self):
        self.db.run('UPDATE movies SET poster=?,title=?,description=? WHERE id=1',('poster-id','🎬'*100,'🎬'*450))
        await self.app.card(2,1)
        self.assertLessEqual(utf16_length(self.bot.send_photo.call_args.kwargs['caption']),1024)
        self.assertIn('🎬'*450,self.bot.send_message.call_args.args[1])
        self.bot.send_video.assert_awaited_once()

    async def test_deleted_replacement_never_reports_success(self):
        await self.app.admin_action(1,['asset_replace','1'])
        self.db.run('DELETE FROM assets WHERE id=1')
        m=self.msg();m.video=SimpleNamespace(file_id='new-file')
        await self.app.message(m)
        self.assertIn('o‘chirilgan',self.bot.send_message.call_args.args[1])
        self.assertIsNone(self.db.state(1))

    async def test_large_ids_and_forged_callback_do_not_change_data(self):
        await self.app.admin_action(1,['admin_add'])
        await self.app.message(self.msg('9'*80+' editor'))
        self.assertEqual(self.db.all('SELECT * FROM admins'),[])
        await self.click('a:delete_yes:'+'9'*40)
        self.assertIsNotNone(self.db.movie(1))
        await self.click('list:all:0:-1',uid=2)
        self.assertIn('eskirgan',self.bot.send_message.call_args.args[1])

    async def test_channel_delete_needs_confirmation_and_permission(self):
        cid=self.db.run('INSERT INTO channels(chat_id,title,url) VALUES(-100,?,?)',('Channel','https://t.me/test'))
        await self.click(f'a:channel_delete:{cid}')
        self.assertTrue(self.db.all('SELECT * FROM channels'))
        await self.click(f'a:channel_delete_yes:{cid}',uid=2)
        self.assertTrue(self.db.all('SELECT * FROM channels'))
        await self.click(f'a:channel_delete_yes:{cid}')
        self.assertFalse(self.db.all('SELECT * FROM channels'))

    async def test_report_resolution_preserves_report_and_requires_role(self):
        rid=self.db.run('INSERT INTO reports(user_id,movie_id,text,created) VALUES(2,1,?,?)',('Missing video',self.db.now()))
        await self.click(f'a:resolve_report:{rid}',uid=2)
        self.assertEqual(self.db.one('SELECT resolved FROM reports')['resolved'],0)
        await self.click(f'a:resolve_report:{rid}')
        row=self.db.one('SELECT * FROM reports')
        self.assertEqual(row['resolved'],1);self.assertEqual(row['text'],'Missing video')
        await self.app.admin_action(1,['reports','0'])
        self.assertIn('yo‘q',self.bot.send_message.call_args.args[1])

    async def test_admin_lookup_includes_hidden_movies(self):
        self.db.run('UPDATE movies SET hidden=1 WHERE id=1')
        await self.app.admin_action(1,['lookup'])
        await self.app.message(self.msg('101'))
        self.assertIn('yashirilgan',self.bot.send_message.call_args.args[1])
        self.assertIsNone(self.db.state(1))
        await self.click('a:lookup',uid=2)
        self.assertIsNone(self.db.state(2))

    async def test_channel_and_admin_menus_paginate(self):
        for i in range(12):
            self.db.run('INSERT INTO channels(chat_id,title,url) VALUES(?,?,?)',(-100-i,str(i),'https://t.me/test'))
            self.db.run('INSERT INTO admins VALUES(?,?)',(100+i,'editor'))
        await self.app.channel_menu(1)
        self.assertIn('a:channels:1',[b.callback_data for b in self.buttons()])
        await self.app.admin_action(1,['admins'])
        self.assertIn('a:admins:1',[b.callback_data for b in self.buttons()])
        await self.app.channel_menu(1,1)
        self.assertIn('a:channels:0',[b.callback_data for b in self.buttons()])

    async def test_video_flood_wait_retries_once_and_counts_one_delivery(self):
        self.bot.send_video.side_effect=[TelegramRetryAfter(method=SendVideo(chat_id=2,video='x'),message='wait',retry_after=0),None]
        await self.app.send_asset(2,1)
        self.assertEqual(self.bot.send_video.await_count,2)
        self.assertEqual(self.db.one('SELECT COUNT(*) n FROM deliveries')['n'],1)

    async def test_long_flood_wait_never_reports_delivery(self):
        self.bot.send_video.side_effect=TelegramRetryAfter(method=SendVideo(chat_id=2,video='x'),message='wait',retry_after=20)
        await self.app.send_asset(2,1)
        self.bot.send_video.assert_awaited_once()
        self.assertEqual(self.db.one('SELECT COUNT(*) n FROM deliveries')['n'],0)
        self.assertIn('20 soniyadan',self.bot.send_message.call_args.args[1])

    async def test_membership_parallelism_is_bounded_and_fails_closed(self):
        active=peak=0
        async def membership(chat,uid):
            nonlocal active,peak
            active+=1;peak=max(peak,active)
            await asyncio.sleep(0)
            active-=1
            return SimpleNamespace(status='left' if chat==-105 else 'member')
        for i in range(10):
            self.db.run('INSERT INTO channels(chat_id,title,url,required) VALUES(?,?,?,1)',(-100-i,str(i),'https://t.me/test'))
        self.bot.get_chat_member.side_effect=membership
        self.assertFalse(await self.app.gate(2,'asset:1'))
        self.assertGreater(peak,1);self.assertLessEqual(peak,4)
        self.assertEqual(self.bot.get_chat_member.await_count,10)

    async def test_user_operations_serialize_without_blocking_other_users(self):
        guard=Guard(self.app)
        entered=asyncio.Event();release=asyncio.Event();other=asyncio.Event()
        order=[]
        async def handler(event,data):
            if event.message_id==1:
                order.append('first-start');entered.set();await release.wait();order.append('first-end')
            elif event.message_id==2: order.append('second')
            else: other.set()
        def message(mid,uid=1):
            return Message(message_id=mid,date=datetime.now(timezone.utc),chat=Chat(id=uid,type='private'),
                from_user=User(id=uid,is_bot=False,first_name='Test'),text='x')
        first=asyncio.create_task(guard(handler,message(1),{}))
        await asyncio.wait_for(entered.wait(),1)
        second=asyncio.create_task(guard(handler,message(2),{}))
        third=asyncio.create_task(guard(handler,message(3,2),{}))
        await asyncio.wait_for(other.wait(),1)
        self.assertEqual(order,['first-start'])
        release.set();await asyncio.gather(first,second,third)
        self.assertEqual(order,['first-start','first-end','second'])
        self.assertEqual(len(guard.locks),0)

    async def test_guard_rechecks_block_after_waiting(self):
        guard=Guard(self.app);started=asyncio.Event();release=asyncio.Event()
        # Lightweight events keep outbound blocked-message replies fully mocked.
        event=SimpleNamespace(from_user=SimpleNamespace(id=2),answer=AsyncMock())
        count=0
        async def handler(event,data):
            nonlocal count
            count+=1;started.set();await release.wait()
        first=asyncio.create_task(guard(handler,event,{}))
        await asyncio.wait_for(started.wait(),1)
        second=asyncio.create_task(guard(handler,event,{}))
        await asyncio.sleep(0)
        self.db.run('UPDATE users SET blocked=1 WHERE id=2')
        release.set();await asyncio.gather(first,second)
        self.assertEqual(count,1)
        event.answer.assert_awaited_once()

    async def test_shutdown_waits_for_inflight_user_operation(self):
        entered=asyncio.Event();release=asyncio.Event()
        event=SimpleNamespace(from_user=SimpleNamespace(id=2))
        async def handler(event,data):
            entered.set();await release.wait()
            self.db.set_state(2,{'type':'finished'})
        update=asyncio.create_task(self.app.guard(handler,event,{}))
        await asyncio.wait_for(entered.wait(),1)
        drain=asyncio.create_task(self.app.drain_updates())
        await asyncio.sleep(0)
        self.assertFalse(drain.done())
        release.set();await asyncio.gather(update,drain)
        self.assertEqual(self.db.state(2),{'type':'finished'})
        self.assertFalse(self.app.guard.active)

    async def test_worker_failure_stops_polling_for_host_restart(self):
        cancelled=asyncio.Event()
        async def worker():
            await asyncio.sleep(0)
            raise RuntimeError('storage unavailable')
        async def polling(*args,**kwargs):
            try: await asyncio.Event().wait()
            finally: cancelled.set()
        app=SimpleNamespace(queue_worker=worker,dp=SimpleNamespace(start_polling=polling,stop_polling=AsyncMock()))
        bot=SimpleNamespace(get_me=AsyncMock(return_value=SimpleNamespace(username='test_bot')),
            get_webhook_info=AsyncMock(return_value=SimpleNamespace(url='')),set_my_commands=AsyncMock())
        with patch('bot.App',return_value=app):
            with self.assertRaisesRegex(RuntimeError,'storage unavailable'):
                await asyncio.wait_for(run_bot(bot,self.db,{1}),1)
        self.assertTrue(cancelled.is_set())

class MigrationTests(unittest.TestCase):
    def test_old_database_upgrade_is_idempotent_and_preserves_data(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'old.db'
            with sqlite3.connect(path) as conn:
                conn.executescript(SCHEMA)
                conn.execute("INSERT INTO users(id,joined,last_seen) VALUES(1,'2026','2026')")
                conn.execute("INSERT INTO reports(user_id,text,created) VALUES(1,'Original report','2026')")
            for _ in range(2):
                db=Store(path)
                self.assertEqual(db.one('SELECT * FROM reports')['text'],'Original report')
                self.assertEqual(db.one('SELECT * FROM reports')['resolved'],0)
                self.assertEqual(db.one('PRAGMA integrity_check')['integrity_check'],'ok')
                db.close()

    def test_telegram_id_range_and_utf16(self):
        self.assertEqual(telegram_id('5771496552'),5771496552)
        for value in ('0','-1','１２３','9'*40,str(2**52)):
            with self.assertRaises(ValueError):telegram_id(value)
        self.assertEqual(utf16_length('a🎬'),3)

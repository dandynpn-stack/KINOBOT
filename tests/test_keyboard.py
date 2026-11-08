import json
import tempfile
import unittest
from datetime import datetime,timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest
from aiogram.methods import SendMessage,SendVideo
from aiogram.types import Message,Update,Chat,User,ReplyKeyboardMarkup
from bot import App
from storage import Store
from backup import restore
from test_bot import movie,asset,FakeSession

class KeyboardTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.db=Store(':memory:');self.db.touch(1);self.db.touch(2)
        self.bot=SimpleNamespace(id=123,send_message=AsyncMock(),send_photo=AsyncMock(),
            send_video=AsyncMock(),send_document=AsyncMock(),copy_message=AsyncMock(),
            get_chat_member=AsyncMock(return_value=SimpleNamespace(status='member')))
        self.app=App(self.bot,self.db,{1});self.app.username='test_bot'
        self.mid=self.db.create_with_asset(movie(),asset())

    async def asyncTearDown(self):self.db.close()

    async def press(self,label,uid=1):
        await self.app.message(SimpleNamespace(from_user=SimpleNamespace(id=uid),text=label,
            photo=None,video=None,document=None))

    async def test_main_admin_catalog_and_video_use_reply_keyboard(self):
        await self.app.home(1)
        self.assertIsInstance(self.bot.send_message.call_args.kwargs['reply_markup'],ReplyKeyboardMarkup)
        await self.press('⚙️ Admin panel')
        self.assertIn('➕ Kino / serial qo‘shish',self.db.keyboard(1)['actions'])
        await self.press('🏠 Menyu')
        await self.press('🎬 Kinolar')
        label=next(label for label in self.db.keyboard(1)['actions'] if label.startswith('101 ·'))
        await self.press(label)
        self.bot.send_video.assert_awaited_once()
        self.assertIsInstance(self.bot.send_video.call_args.kwargs['reply_markup'],ReplyKeyboardMarkup)
        await self.press('ℹ️ Kino haqida')
        self.assertIn('⭐ Baho berish',self.db.keyboard(1)['actions'])
        self.bot.send_video.assert_awaited_once()

    async def test_wizard_keyboard_input_does_not_recurse(self):
        await self.app.home(1);await self.press('⚙️ Admin panel')
        await self.press('➕ Kino / serial qo‘shish')
        await self.press('900');await self.press('Yangi film');await self.press('🎬 Kino')
        for _ in range(7):await self.press('⏭ O‘tkazib yuborish')
        self.assertEqual(self.db.state(1)['type'],'asset')
        await self.press('O‘zbekcha');await self.press('720p')
        self.assertEqual(self.db.state(1)['data']['language'],'O‘zbekcha')
        self.assertEqual(self.db.state(1)['data']['quality'],'720p')
        self.assertNotIn('720p',self.db.keyboard(1)['actions'])
        await self.app.message(SimpleNamespace(from_user=SimpleNamespace(id=1),text=None,photo=None,
            video=SimpleNamespace(file_id='fresh'),document=None))
        self.assertIsNotNone(self.db.by_code('900'))

    async def test_permission_is_rechecked_for_stored_admin_keyboard(self):
        self.db.run("INSERT INTO admins VALUES(2,'editor')")
        await self.app.admin_home(2)
        self.db.run('DELETE FROM admins WHERE user_id=2')
        await self.press('➕ Kino / serial qo‘shish',2)
        self.assertIsNone(self.db.state(2))
        self.assertIn('huquqingiz',self.bot.send_message.call_args.args[1])

    async def test_keyboard_maps_are_private_to_each_user(self):
        await self.app.admin_home(1);await self.app.home(2)
        await self.press('➕ Kino / serial qo‘shish',2)
        self.assertIsNone(self.db.state(2))
        self.assertNotIn('➕ Kino / serial qo‘shish',self.db.keyboard(2)['actions'])

    async def test_channel_link_keeps_subscription_check_button(self):
        self.db.run("INSERT INTO channels(chat_id,title,url,required) VALUES(-100,'Kanal','https://t.me/test',1)")
        self.bot.get_chat_member.return_value=SimpleNamespace(status='left')
        await self.app.open_code(2,'101')
        before=self.db.keyboard(2)
        await self.press('Kanal',2)
        self.assertIn('https://t.me/test',self.bot.send_message.call_args.args[1])
        self.assertEqual(self.db.keyboard(2),before)
        self.bot.get_chat_member.return_value=SimpleNamespace(status='member')
        await self.press('✅ Obunani tekshirish',2)
        self.bot.send_video.assert_awaited_once()

    async def test_urls_and_favorites_are_available_from_keyboard(self):
        self.db.run("UPDATE movies SET trailer='https://example.com/trailer' WHERE id=1")
        await self.app.card(2,1,auto_send=False)
        await self.press('🎞 Treyler',2)
        self.assertIn('https://example.com/trailer',self.bot.send_message.call_args.args[1])
        await self.press('↗️ Do‘stga ulashish',2)
        self.assertIn('https://t.me/share/url?',self.bot.send_message.call_args.args[1])
        await self.press('❤️ Sevimlilar',2)
        self.assertTrue(self.db.one("SELECT 1 FROM marks WHERE user_id=2 AND kind='favorite'"))

    async def test_failed_keyboard_delivery_keeps_previous_mapping(self):
        await self.app.home(1);before=self.db.keyboard(1)
        self.bot.send_message.side_effect=TelegramBadRequest(method=SendMessage(chat_id=1,text='x'),message='failed')
        with self.assertRaises(TelegramBadRequest):await self.app.admin_home(1)
        self.assertEqual(self.db.keyboard(1),before)

    async def test_duplicate_labels_have_distinct_routes(self):
        await self.app.say(1,'Choose',[[('Same','view:1'),('Same','view:2')]])
        self.assertEqual(self.db.keyboard(1)['actions']['Same'],'view:1')
        self.assertEqual(self.db.keyboard(1)['actions']['Same (2)'],'view:2')

    async def test_reports_keep_all_resolution_buttons(self):
        for i in range(3):self.db.run('INSERT INTO reports(user_id,movie_id,text,created) VALUES(1,1,?,?)',(str(i),self.db.now()))
        await self.app.admin_action(1,['reports','0'])
        actions=self.db.keyboard(1)['actions']
        self.assertEqual(sum(a.startswith('a:resolve_report:') for a in actions.values()),3)
        await self.press('✅ #2 — hal qilindi')
        self.assertEqual(self.db.one('SELECT resolved FROM reports WHERE id=2')['resolved'],1)
        self.assertEqual(self.db.one('SELECT resolved FROM reports WHERE id=1')['resolved'],0)

    async def test_notifications_do_not_replace_admin_keyboard(self):
        mid=self.db.create_with_asset(movie('S1','series'),asset(season=1,episode=1))
        self.db.mark(1,mid,'follow')
        await self.app.admin_action(1,['add']);before=self.db.keyboard(1)
        await self.app.owner_notice('Import succeeded')
        await self.app.queue_item({'kind':'episode','user_id':1,'payload':json.dumps({'movie_id':mid,'season':1,'episode':1})})
        self.assertEqual(self.db.keyboard(1),before)
        self.assertNotIn('reply_markup',self.bot.send_message.call_args.kwargs)

    async def test_keyboard_persists_across_restart_and_restore_clears_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'app.db';backup=Path(tmp)/'backup.db';target=Path(tmp)/'restored.db'
            db=Store(path);db.touch(2)
            app=App(self.bot,db,{1});await app.home(2)
            db.backup(backup);db.close()
            db=Store(path);app=App(self.bot,db,{1})
            await app.message(SimpleNamespace(from_user=SimpleNamespace(id=2),text='🔔 Sozlamalar'))
            self.assertIn('notify',db.keyboard(2)['actions'].values())
            db.close();restore(backup,target)
            db=Store(target);self.assertIsNone(db.keyboard(2));db.close()

    async def test_real_dispatcher_routes_keyboard_text(self):
        session=FakeSession();bot=Bot('123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijk',session=session)
        app=App(bot,self.db,{1});app.username='test_bot'
        for i,text in enumerate(['/start','🎬 Kinolar','101 · Sinov kinosi','ℹ️ Kino haqida','⭐ Baho berish','5 ⭐'],1):
            m=Message(message_id=i,date=datetime.now(timezone.utc),chat=Chat(id=2,type='private'),
                from_user=User(id=2,is_bot=False,first_name='Test'),text=text)
            await app.dp.feed_update(bot,Update(update_id=i,message=m))
        self.assertEqual(self.db.one('SELECT score FROM ratings')['score'],5)
        self.assertEqual(sum(isinstance(m,SendVideo) for m in session.methods),1)
        for method in session.methods:
            markup=getattr(method,'reply_markup',None)
            if markup is not None:self.assertIsInstance(markup,ReplyKeyboardMarkup)
        await bot.session.close()

"""Kino Bot: private-chat catalog, admin workflows and persistent delivery queue."""
import asyncio
import contextlib
import json
import logging
import os
import secrets
import sqlite3
import psycopg2
import tempfile
import time
import traceback
from collections import defaultdict, deque
from types import SimpleNamespace
from weakref import WeakValueDictionary
from pathlib import Path
from urllib.parse import quote

from aiogram import Bot, Dispatcher, BaseMiddleware
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError, TelegramRetryAfter, TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import (Message, CallbackQuery, KeyboardButton,
    ReplyKeyboardMarkup, FSInputFile, BotCommand, ErrorEvent)
from dotenv import load_dotenv

from core import (FIELDS, WIZARD, OPTIONAL, allowed, code_value, validate,
                  https_url, parse_channel_caption, is_member, telegram_id, utf16_length)
from storage import Store
from runtime import InstanceLock

log = logging.getLogger('kino_bot')
PAGE = 8
CHOICES = {
    'kind': [('🎬 Kino', 'movie'), ('📺 Serial', 'series')],
    'language': [('O‘zbekcha', 'O‘zbekcha'), ('Ruscha', 'Ruscha'), ('Inglizcha', 'Inglizcha')],
    'quality': [('480p', '480p'), ('720p', '720p'), ('1080p', '1080p'), ('4K', '4K')],
}

def rows_of(items, n=1):
    return [items[i:i+n] for i in range(0,len(items),n)]

class Guard(BaseMiddleware):
    def __init__(self, app):
        self.app = app
        self.hits = defaultdict(deque)
        self.last_clean = 0.0
        self.warned = {}
        self.locks = WeakValueDictionary()
        self.active = set()

    async def __call__(self, handler, event, data):
        if isinstance(event, Message) and event.chat.type != 'private':
            return
        if isinstance(event, CallbackQuery) and (not isinstance(event.message, Message) or event.message.chat.type != 'private'):
            return
        uid = event.from_user.id
        now = time.monotonic()
        if now-self.last_clean > 60:
            for key in list(self.hits):
                if not self.hits[key] or now-self.hits[key][-1] > 10:
                    del self.hits[key]
                    self.warned.pop(key, None)
            self.last_clean = now
        q = self.hits[uid]
        while q and now-q[0] > 5:
            q.popleft()
        if len(q) >= 8:
            if isinstance(event, CallbackQuery):
                with contextlib.suppress(TelegramAPIError):
                    await event.answer('Biroz kuting, so‘ng qayta bosing.', show_alert=True)
            elif now-self.warned.get(uid, -10) > 5:
                self.warned[uid] = now
                with contextlib.suppress(TelegramAPIError):
                    await event.answer('Juda tez yuboryapsiz. 5 soniyadan keyin davom eting.')
            return
        q.append(now)
        # Serialize a user's messages AND callbacks, while other users proceed.
        lock = self.locks.setdefault(uid, asyncio.Lock())
        task=asyncio.current_task()
        self.active.add(task)
        try:
            async with lock:
                user = self.app.db.touch(uid)
                if user['blocked'] and uid not in self.app.owners:
                    with contextlib.suppress(TelegramAPIError):
                        if isinstance(event, CallbackQuery):
                            await event.answer('Botdan foydalanish cheklangan.', show_alert=True)
                        else:
                            await event.answer('Botdan foydalanish cheklangan.')
                    return
                return await handler(event,data)
        finally:
            self.active.discard(task)


class App:
    def __init__(self, bot, db, owners, title='Kino Bot'):
        self.bot, self.db, self.owners, self.title = bot, db, set(owners), title
        self.username = ''
        self.membership_slots = asyncio.Semaphore(4)
        self.dp = Dispatcher()
        guard = self.guard = Guard(self)
        self.dp.shutdown.register(self.drain_updates)
        self.dp.message.outer_middleware(guard)
        self.dp.callback_query.outer_middleware(guard)
        self.dp.message.register(self.start, Command('start'))
        self.dp.message.register(self.cancel, Command('cancel'))
        self.dp.message.register(self.help_command, Command('help'))
        self.dp.message.register(self.my_id, Command('id'))
        self.dp.message.register(self.admin_command, Command('admin'))
        self.dp.message.register(self.message)
        self.dp.callback_query.register(self.callback)
        self.dp.channel_post.register(self.channel_post)
        self.dp.errors.register(self.error)

    async def drain_updates(self, **kwargs):
        tasks=tuple(self.guard.active)
        if tasks:
            _, pending=await asyncio.wait(tasks,timeout=5)
            for task in pending: task.cancel()
            await asyncio.gather(*tasks,return_exceptions=True)

    def role(self, uid):
        return self.db.role(uid,self.owners)

    def permit(self, uid, permission):
        return allowed(self.role(uid),permission)

    def prepare_keyboard(self,uid,rows):
        state=self.db.state(uid)
        previous=self.db.keyboard(uid)
        if rows is None:
            if previous and previous.get('scope')==state:
                return None, None  # Keep the currently visible keyboard.
            rows=[[('✖️ Bekor qilish','admin' if self.role(uid) else 'home')]] if state else []
        rows=[list(row) for row in rows]
        if not any(action=='home' for row in rows for _,action in row):
            rows.append([('🏠 Menyu','home')])
        actions={}; buttons=[]
        for row in rows:
            rendered=[]
            for label,action in row:
                # Identical titles (e.g. channels) must still route independently.
                base=label; number=2
                while label in actions:
                    label=f'{base} ({number})'; number+=1
                actions[label]=action
                rendered.append(KeyboardButton(text=label))
            if rendered: buttons.append(rendered)
        return ReplyKeyboardMarkup(keyboard=buttons,resize_keyboard=True,
            is_persistent=True,one_time_keyboard=False,
            input_field_placeholder='Tugmani tanlang yoki kino kodini yozing'), {'actions':actions,'scope':state}

    async def send_with_keyboard(self,sender,uid,*args,rows=None,**kwargs):
        markup,saved=self.prepare_keyboard(uid,rows)
        result=await sender(uid,*args,reply_markup=markup,**kwargs)
        if saved is not None:
            self.db.set_keyboard(uid,saved)
        return result

    async def say(self, uid, text, rows=None):
        return await self.send_with_keyboard(self.bot.send_message,uid,text,rows=rows)

    def clear_flow(self, uid):
        self.db.set_state(uid,None)
        self.db.run('UPDATE users SET pending=NULL WHERE id=?',(uid,))

    async def start(self, m):
        self.clear_flow(m.from_user.id)
        arg = (m.text or '').split(maxsplit=1)
        if len(arg)==2 and arg[1].startswith('film_'):
            await self.open_code(m.from_user.id,arg[1][5:])
        else:
            await self.home(m.from_user.id)

    async def cancel(self,m):
        self.clear_flow(m.from_user.id)
        await self.home(m.from_user.id)

    async def help_command(self,m):
        self.clear_flow(m.from_user.id)
        await self.help(m.from_user.id)

    async def help(self,uid):
        await self.say(uid,'Kino olish: kodini yozing, masalan 101.\n'
                       'Yoki «Kino olish» tugmasini bosing.\n'
                       'Serial: fasl → qism → til va sifat.\n'
                       'Obuna so‘ralsa, kanallarga kiring va Obunani tekshirish tugmasini bosing.\n\n'
                       '/start — bosh menyu\n/cancel — joriy amalni bekor qilish\n'
                       '/id — Telegram ID\n/admin — admin panel',[[('🏠 Menyu','home')]])

    async def my_id(self,m):
        await m.answer(f'Sizning Telegram ID: {m.from_user.id}')

    async def home(self,uid):
        rows = [[('🎬 Kino olish','ask_code')],
                [('❓ Yordam','help')]]
        if self.role(uid):
            rows.append([('⚙️ Admin panel','admin')])
        await self.say(uid,f'{self.title}\n\nKino olish uchun kodni yuboring yoki tugmani bosing.',rows)

    async def gate(self,uid,action):
        channels = self.db.all('SELECT * FROM channels WHERE required=1 ORDER BY id')
        async def check(ch):
            async with self.membership_slots:
                try:
                    member = await self.bot.get_chat_member(ch['chat_id'],uid)
                    return ch, is_member(member)
                except TelegramAPIError as exc:
                    log.warning('Membership unavailable channel=%s error=%s',ch['chat_id'],type(exc).__name__)
                    return ch, None
        results = await asyncio.gather(*(check(ch) for ch in channels))
        missing = [ch for ch, status in results if status is False]
        failure = any(status is None for _, status in results)
        if not missing and not failure:
            self.db.run('UPDATE users SET pending=NULL WHERE id=?',(uid,))
            return True
        self.db.run('UPDATE users SET pending=? WHERE id=?',(action,uid))
        rows = [[(ch['title'],ch['url'])] for ch in missing]
        rows.append([('✅ Obunani tekshirish','check')])
        text = 'Kinoni olish uchun quyidagi kanallarga a’zo bo‘ling.'
        if failure:
            text += '\nObunani tekshirish hozir ishlamayapti. Admin kanal huquqlarini tekshirishi kerak.'
        await self.say(uid,text,rows)
        return False

    async def open_code(self,uid,code):
        try:
            code=code_value(code)
        except (ValueError,OverflowError) as e:
            await self.say(uid,str(e) if isinstance(e,ValueError) else 'Raqam juda katta. To‘g‘ri qiymat kiriting.'); return
        movie=self.db.by_code(code)
        if not movie:
            await self.say(uid,'Bu kod bo‘yicha kino topilmadi.'); return
        await self.card(uid,movie['id'])

    async def card(self,uid,mid,auto_send=True):
        movie=self.db.movie(mid)
        if not movie:
            await self.say(uid,'Kino mavjud emas yoki vaqtincha yashirilgan.'); return
        if not await self.gate(uid,f"{'open' if auto_send else 'view'}:{mid}"):
            return
        score=self.db.one('SELECT AVG(score) avg,COUNT(*) n FROM ratings WHERE movie_id=?',(mid,))
        watched=self.db.one('SELECT COUNT(*) n FROM deliveries WHERE movie_id=?',(mid,))['n']
        rating=f"{score['avg']:.1f}/5 ({score['n']} baho)" if score['n'] else 'Hali baholanmagan'
        text=(f"{movie['title']}\nKod: {movie['code']}\n"
              f"{movie['genre'] or 'Janr kiritilmagan'} · {movie['year'] or 'Yil kiritilmagan'} · {movie['country'] or 'Davlat kiritilmagan'}\n"
              f"Davomiylik: {movie['duration'] or '—'} daqiqa\n⭐ {rating} · {watched} marta olindi\n\n{movie['description']}")
        link=f'https://t.me/{self.username}?start=film_{movie["code"]}'
        marks={r['kind'] for r in self.db.all('SELECT kind FROM marks WHERE user_id=? AND movie_id=?',(uid,mid))}
        rows=[[('▶️ Kino olish' if movie['kind']=='movie' else '📺 Fasllar',f'play:{mid}')],
              [('❤️ '+('Saqlangan' if 'favorite' in marks else 'Sevimlilar'),f'mark:favorite:{mid}'),
               ('🕒 '+('Saqlangan' if 'later' in marks else 'Keyin ko‘raman'),f'mark:later:{mid}')],
              [('⭐ Baho berish',f'rate:{mid}'),('⚠️ Xatolik',f'report:{mid}')],
              [('↗️ Do‘stga ulashish','https://t.me/share/url?url='+quote(link,safe=''))]]
        if movie['kind']=='series':
            rows.append([('🔔 '+('Kuzatishni to‘xtatish' if 'follow' in marks else 'Yangi qismlarni kuzatish'),f'mark:follow:{mid}')])
        if movie['trailer']:
            rows.append([('🎞 Treyler',movie['trailer'])])
        rows.append([('🏠 Bosh menyu','home')])
        if movie['poster']:
            try:
                if utf16_length(text) <= 1024:
                    await self.send_with_keyboard(self.bot.send_photo,uid,movie['poster'],caption=text,rows=rows)
                else:
                    await self.bot.send_photo(uid,movie['poster'],caption=movie['title'])
                    await self.say(uid,text,rows)
            except TelegramBadRequest:
                await self.say(uid,text,rows)
        else:
            await self.say(uid,text,rows)
        variants=self.db.all('SELECT id FROM assets WHERE movie_id=?',(mid,))
        if auto_send and movie['kind']=='movie' and len(variants)==1:
            await self.send_asset(uid,variants[0]['id'],check_gate=False)

    async def send_asset(self,uid,aid,check_gate=True):
        asset=self.db.one('SELECT a.*,m.title,m.code,m.hidden FROM assets a JOIN movies m ON m.id=a.movie_id WHERE a.id=?',(aid,))
        if not asset or asset['hidden']:
            await self.say(uid,'Bu video mavjud emas.'); return
        if check_gate and not await self.gate(uid,f'asset:{aid}'):
            return
        caption=f"{asset['title']}\nKod: {asset['code']} · {asset['language']} · {asset['quality']}"
        if asset['season']:
            caption+=f"\n{asset['season']}-fasl, {asset['episode']}-qism"
        sender=self.bot.send_video if asset['media_type']=='video' else self.bot.send_document
        rows=[]
        if asset['season']:
            for label, op, order in [('⬅️ Oldingi qism','<','DESC'),('Keyingi qism ➡️','>','ASC')]:
                neighbor=self.db.one(f'SELECT season,episode FROM assets WHERE movie_id=? '
                    f'AND (season,episode) {op} (?,?) ORDER BY season {order},episode {order} LIMIT 1',
                    (asset['movie_id'],asset['season'],asset['episode']))
                if neighbor:
                    rows.append([(label,f"variants:{asset['movie_id']}:{neighbor['season']}:{neighbor['episode']}:0")])
        rows.append([('ℹ️ Kino haqida',f"view:{asset['movie_id']}"),('🏠 Menyu','home')])
        try:
            try:
                await self.send_with_keyboard(sender,uid,asset['file_id'],caption=caption,rows=rows)
            except TelegramRetryAfter as exc:
                if exc.retry_after > 3:
                    await self.say(uid,f'Telegram band. {exc.retry_after} soniyadan keyin qayta urinib ko‘ring.')
                    return
                await asyncio.sleep(exc.retry_after+0.1)
                await self.send_with_keyboard(sender,uid,asset['file_id'],caption=caption,rows=rows)
        except TelegramAPIError as exc:
            log.warning('Delivery failed asset=%s error=%s',aid,type(exc).__name__)
            await self.say(uid,'Video yuborilmadi. Birozdan keyin qayta urinib ko‘ring yoki xatolik yuboring.',
                           [[('⚠️ Xatolik yuborish',f"report:{asset['movie_id']}")]])
            return
        self.db.run('INSERT INTO deliveries(user_id,movie_id,sent_at) VALUES(?,?,?)',(uid,asset['movie_id'],self.db.now()))

    async def play(self,uid,mid):
        movie=self.db.movie(mid)
        if not movie:
            await self.say(uid,'Kino topilmadi.'); return
        if not await self.gate(uid,f'play:{mid}'):
            return
        if movie['kind']=='series':
            await self.seasons(uid,mid,0)
        else:
            await self.variants(uid,mid,0,0,0)

    async def seasons(self,uid,mid,page):
        if not self.db.movie(mid): return
        data=self.db.all('SELECT DISTINCT season FROM assets WHERE movie_id=? ORDER BY season LIMIT ? OFFSET ?', (mid,PAGE+1,page*PAGE))
        items=[(f"{r['season']}-fasl",f"eps:{mid}:{r['season']}:0") for r in data[:PAGE]]
        rows=rows_of(items,2)
        self.pager(rows,page,len(data)>PAGE,f'seasons:{mid}:')
        rows.append([('⬅️ Kino',f'view:{mid}')])
        await self.say(uid,'Faslni tanlang:' if data else 'Hali qismlar qo‘shilmagan.',rows)

    async def episodes(self,uid,mid,season,page):
        if not self.db.movie(mid): return
        data=self.db.all('SELECT DISTINCT episode FROM assets WHERE movie_id=? AND season=? ORDER BY episode LIMIT ? OFFSET ?',
                         (mid,season,PAGE+1,page*PAGE))
        rows=rows_of([(f"{r['episode']}-qism",f"variants:{mid}:{season}:{r['episode']}:0") for r in data[:PAGE]],2)
        self.pager(rows,page,len(data)>PAGE,f'eps:{mid}:{season}:')
        rows.append([('⬅️ Fasllar',f'seasons:{mid}:0')])
        await self.say(uid,f'{season}-fasl: qismni tanlang.',rows)

    async def variants(self,uid,mid,season,episode,page):
        if not self.db.movie(mid): return
        data=self.db.all('SELECT * FROM assets WHERE movie_id=? AND season=? AND episode=? ORDER BY language,quality,id LIMIT ? OFFSET ?',
                        (mid,season,episode,PAGE+1,page*PAGE))
        rows=[[(f"{a['language']} · {a['quality']}",f"asset:{a['id']}")] for a in data[:PAGE]]
        self.pager(rows,page,len(data)>PAGE,f'variants:{mid}:{season}:{episode}:')
        rows.append([('⬅️ Kino',f'view:{mid}')])
        await self.say(uid,'Til va sifatni tanlang:' if data else 'Hozircha video qo‘shilmagan.',rows)

    @staticmethod
    def pager(rows,page,more,prefix):
        nav=[]
        if page>0: nav.append(('⬅️ Oldingi',prefix+str(page-1)))
        if more: nav.append(('Keyingi ➡️',prefix+str(page+1)))
        if nav: rows.append(nav)

    async def filters(self,uid,field,page):
        if field not in ('genre','year','country'): return
        data=self.db.all(f"SELECT {field} val,MIN(id) ref FROM movies WHERE hidden=0 AND {field} NOT IN ('',0) GROUP BY {field} ORDER BY {field} LIMIT ? OFFSET ?",(PAGE+1,page*PAGE))
        rows=[[(str(r['val']),f"list:{field}:{r['ref']}:0")] for r in data[:PAGE]]
        self.pager(rows,page,len(data)>PAGE,f'filter:{field}:')
        rows.append([('🏠 Menyu','home')])
        await self.say(uid,'Tanlang:' if data else 'Hali ma’lumot qo‘shilmagan.',rows)

    async def listing(self,uid,mode,ref,page):
        args=[]; where='m.hidden=0'; order='m.id DESC'
        if mode=='series': where+=" AND m.kind='series'"
        elif mode=='top': order='(SELECT COUNT(*) FROM deliveries d WHERE d.movie_id=m.id) DESC,m.id DESC'
        elif mode in ('genre','year','country'):
            row=self.db.movie(ref)
            if not row: await self.say(uid,'Bu bo‘lim o‘zgargan. Menyudan qayta tanlang.'); return
            where+=f' AND m.{mode}=?'; args.append(row[mode])
        elif mode in ('favorite','later'):
            where+=' AND EXISTS(SELECT 1 FROM marks t WHERE t.movie_id=m.id AND t.user_id=? AND t.kind=?)'; args += [uid,mode]
        elif mode=='history':
            where+=' AND EXISTS(SELECT 1 FROM deliveries d WHERE d.movie_id=m.id AND d.user_id=?)'; args.append(uid)
            order='(SELECT MAX(d.id) FROM deliveries d WHERE d.movie_id=m.id AND d.user_id=?) DESC'; args.append(uid)
        elif mode!='all': return
        data=self.db.all(f'SELECT m.* FROM movies m WHERE {where} ORDER BY {order} LIMIT ? OFFSET ?',args+[PAGE+1,page*PAGE])
        rows=[[(f"{m['code']} · {m['title']}",f"open:{m['id']}")] for m in data[:PAGE]]
        self.pager(rows,page,len(data)>PAGE,f'list:{mode}:{ref}:')
        rows.append([('🏠 Menyu','home')])
        await self.say(uid,'Kinoni tanlang:' if data else 'Bu ro‘yxat hozircha bo‘sh.',rows)

    async def settings(self,uid):
        value=self.db.one('SELECT notifications FROM users WHERE id=?',(uid,))['notifications']
        await self.say(uid,'Xabarnomalar: '+('yoqilgan' if value else 'o‘chirilgan'),
                       [[('🔔 '+('O‘chirish' if value else 'Yoqish'),'notify')],[('🏠 Menyu','home')]])

    async def admin_command(self,m):
        self.clear_flow(m.from_user.id)
        await self.admin_home(m.from_user.id)

    async def admin_home(self,uid):
        role=self.role(uid)
        if not role: await self.say(uid,'Bu bo‘lim faqat administratorlar uchun.'); return
        options=[('➕ Kino / serial qo‘shish','add','catalog'),('🎬 Katalogni boshqarish','manage:0','catalog'),
                 ('🔎 Kod orqali boshqarish','lookup','catalog'),
                 ('📣 Kanallar','channels','channels'),('👤 Administratorlar','admins','admins'),
                 ('📊 Statistika','stats','stats'),('📨 Ommaviy xabar','broadcast','broadcast'),
                 ('🚫 Bloklash / ochish','block','block'),('💾 Zaxira nusxa','backup','backup'),
                 ('⚠️ Xatoliklar','reports:0','catalog')]
        rows=[[(label,'a:'+action)] for label,action,perm in options if self.permit(uid,perm)]
        rows.append([('🏠 Menyu','home')])
        await self.say(uid,f'Admin panel · {role}',rows)

    async def managed_movies(self,uid,page):
        data=self.db.all('SELECT * FROM movies ORDER BY id DESC LIMIT ? OFFSET ?',(PAGE+1,page*PAGE))
        rows=[[(f"{'🙈 ' if r['hidden'] else ''}{r['code']} · {r['title']}",f"a:movie:{r['id']}")] for r in data[:PAGE]]
        self.pager(rows,page,len(data)>PAGE,'a:manage:')
        rows.append([('⬅️ Admin panel','admin')])
        await self.say(uid,'Katalog:' if data else 'Katalog bo‘sh.',rows)

    async def managed_movie(self,uid,mid):
        m=self.db.movie(mid,public=False)
        if not m: await self.say(uid,'Kino topilmadi.'); return
        n=self.db.one('SELECT COUNT(*) n FROM assets WHERE movie_id=?',(mid,))['n']
        await self.say(uid,f"{m['title']} · {m['code']}\nVideo variantlari: {n}\nHolati: {'yashirilgan' if m['hidden'] else 'ochiq'}",[
            [('✏️ Tahrirlash',f'a:edit:{mid}'),('➕ Video variant',f'a:asset:{mid}')],
            [('🎞 Videolarni boshqarish',f'a:assets:{mid}:0')],
            [('👁 Ochish' if m['hidden'] else '🙈 Yashirish',f'a:hide:{mid}'),('🗑 O‘chirish',f'a:delete:{mid}')],
            [('📊 Kino statistikasi',f'a:movie_stats:{mid}')], [('⬅️ Katalog','a:manage:0')]])

    async def prompt_field(self,uid,field):
        state=self.db.state(uid)
        rows=[]
        if state:
            state['nonce']=secrets.token_hex(4)
            self.db.set_state(uid,state)
            prefix=f"w:{state['nonce']}:{field}:"
            rows=rows_of([(label,prefix+value) for label,value in CHOICES.get(field,[])],2)
            if field in OPTIONAL:
                rows.append([('⏭ O‘tkazib yuborish',prefix+'/skip')])
        rows.append([('✖️ Bekor qilish','admin')])
        suffix='\nO‘tkazish: /skip' if field in OPTIONAL else ''
        progress=''
        if state and state['type']=='new':
            progress=f"{state['step']+1}/{len(WIZARD)} · "
        await self.say(uid,progress+FIELDS.get(field,{'language':'Til (masalan: O‘zbekcha)','quality':'Sifat (masalan: 720p)',
                     'season':'Fasl raqami','episode':'Qism raqami'}.get(field,field))+':'+suffix+'\nBekor qilish: /cancel',rows)

    async def wizard_choice(self,uid,p):
        state=self.db.state(uid)
        if not state or len(p)!=4 or state.get('nonce')!=p[1]:
            await self.say(uid,'Bu bosqich tugagan. Oxirgi savolga javob bering yoki /cancel yozing.')
            return
        if state['type']=='new': field=WIZARD[state['step']]
        elif state['type']=='edit': field=state['field']
        elif state['type']=='asset' and state['step']<len(state['fields']): field=state['fields'][state['step']]
        else: return
        permitted={value for _,value in CHOICES.get(field,[])}
        if field in OPTIONAL: permitted.add('/skip')
        if p[2]!=field or p[3] not in permitted: return
        await self.message(SimpleNamespace(from_user=SimpleNamespace(id=uid),text=p[3],photo=None),keyboard_input=False)

    async def begin_asset(self,uid,mid,draft=None):
        m=draft or self.db.movie(mid,public=False)
        if not m: await self.say(uid,'Kino topilmadi.'); return
        fields=['language','quality']+(['season','episode'] if m['kind']=='series' else [])
        state={'type':'asset','permission':'catalog','mid':mid,'fields':fields,'step':0,'data':{'season':0,'episode':0}}
        if draft: state['draft']=draft
        self.db.set_state(uid,state)
        await self.prompt_field(uid,fields[0])

    async def channel_menu(self,uid,page=0):
        rows=[]
        channels=self.db.all('SELECT * FROM channels ORDER BY id LIMIT ? OFFSET ?',(PAGE+1,page*PAGE))
        for c in channels[:PAGE]:
            flags=('obuna ' if c['required'] else '')+('import' if c['source'] else '')
            rows.append([(f"{c['title']} ({flags})",f"a:channel:{c['id']}")])
        self.pager(rows,page,len(channels)>PAGE,'a:channels:')
        rows += [[('➕ Kanal qo‘shish','a:channel_add')],[('⬅️ Admin','admin')]]
        await self.say(uid,'Kanallar: obuna — majburiy a’zolik; import — #kino postlaridan qo‘shish.',rows)

    async def stats(self,uid,mid=None):
        if mid is not None:
            row=self.db.movie(mid,public=False)
            if not row: return
            r=self.db.one('SELECT COUNT(*) total,COUNT(DISTINCT user_id) people FROM deliveries WHERE movie_id=?',(mid,))
            rating=self.db.one('SELECT COUNT(*) n,AVG(score) avg FROM ratings WHERE movie_id=?',(mid,))
            await self.say(uid,f"{row['title']}\nYuborilgan: {r['total']}\nNoyob foydalanuvchi: {r['people']}\nBaholar: {rating['n']}\nO‘rtacha: {round(rating['avg'] or 0,2)}")
            return
        users=self.db.one('SELECT COUNT(*) total,SUM(blocked) blocked FROM users')
        today=self.db.one('SELECT COUNT(*) n FROM activity WHERE day=?',(self.db.now()[:10],))['n']
        movies=self.db.one('SELECT COUNT(*) n FROM movies')['n']
        sent=self.db.one('SELECT COUNT(*) n FROM deliveries')['n']
        queue=self.db.all('SELECT state,COUNT(*) n FROM queue GROUP BY state')
        await self.say(uid,f"Foydalanuvchilar: {users['total']}\nBugun faol: {today}\nBloklangan: {users['blocked'] or 0}\nKino/serial: {movies}\nYuborishlar: {sent}\n\nXabar navbati:\n"+'\n'.join(f"{r['state']}: {r['n']}" for r in queue))

    async def callback(self,c):
        uid=c.from_user.id
        with contextlib.suppress(TelegramAPIError):
            await c.answer()
        await self.dispatch_action(uid,c.data or '')

    async def dispatch_action(self,uid,action):
        p=action.split(':')
        try:
            if any(part.lstrip('-').isascii() and part.lstrip('-').isdigit() and
                   (len(part)>16 or not 0 <= int(part) < 2**52) for part in p):
                raise ValueError('Invalid callback number')
            if p[0]=='w':
                await self.wizard_choice(uid,p); return
            if p[0] in ('home','admin','help','ask_code','open','view','play','asset','list','filter','seasons','eps','variants','settings'):
                self.clear_flow(uid)
            if p[0]=='a':
                await self.admin_action(uid,p[1:]); return
            if p[0]=='ask_code':
                await self.say(uid,'Kino kodini yuboring (masalan: 101).')
                return
            if p[0]=='home':
                self.db.set_state(uid,None); await self.home(uid)
            elif p[0]=='admin':
                self.db.set_state(uid,None); await self.admin_home(uid)
            elif p[0]=='help': await self.help(uid)
            elif p[0]=='view': await self.card(uid,int(p[1]),auto_send=False)
            elif p[0]=='open': await self.card(uid,int(p[1]))
            elif p[0]=='play': await self.play(uid,int(p[1]))
            elif p[0]=='asset': await self.send_asset(uid,int(p[1]))
            elif p[0]=='list': await self.listing(uid,p[1],int(p[2]),max(0,int(p[3])))
            elif p[0]=='filter': await self.filters(uid,p[1],max(0,int(p[2])))
            elif p[0]=='seasons': await self.seasons(uid,int(p[1]),max(0,int(p[2])))
            elif p[0]=='eps': await self.episodes(uid,int(p[1]),int(p[2]),max(0,int(p[3])))
            elif p[0]=='variants': await self.variants(uid,int(p[1]),int(p[2]),int(p[3]),max(0,int(p[4])))
            elif p[0]=='check':
                user=self.db.one('SELECT pending FROM users WHERE id=?',(uid,))
                action=(user or {}).get('pending') or 'home'
                if await self.gate(uid,action):
                    kind,_,value=action.partition(':')
                    if kind=='open': await self.card(uid,int(value))
                    elif kind=='view': await self.card(uid,int(value),auto_send=False)
                    elif kind=='asset': await self.send_asset(uid,int(value))
                    elif kind=='play': await self.play(uid,int(value))
                    else: await self.home(uid)
            elif p[0]=='mark':
                mid=int(p[2]); movie=self.db.movie(mid)
                if not movie: await self.say(uid,'Kino topilmadi.'); return
                if p[1]=='follow' and movie['kind']!='series': return
                active=self.db.mark(uid,mid,p[1])
                await self.say(uid,'Ro‘yxatga qo‘shildi.' if active else 'Ro‘yxatdan olib tashlandi.',[[('⬅️ Kino',f'view:{mid}')]])
            elif p[0]=='rate':
                mid=int(p[1])
                if not self.db.movie(mid): return
                await self.say(uid,'Bahoni tanlang:',[[(f'{n} ⭐',f'vote:{mid}:{n}') for n in range(1,6)]])
            elif p[0]=='vote':
                mid,score=int(p[1]),int(p[2])
                if not self.db.movie(mid) or not 1<=score<=5: return
                self.db.run('INSERT INTO ratings VALUES(?,?,?) ON CONFLICT(user_id,movie_id) DO UPDATE SET score=excluded.score',(uid,mid,score))
                await self.say(uid,f'{score} yulduzli bahongiz saqlandi.')
            elif p[0]=='report':
                mid=int(p[1])
                if not self.db.movie(mid): return
                self.db.set_state(uid,{'type':'report','mid':mid})
                await self.say(uid,'Muammoni yozing (1000 belgigacha). Bekor qilish: /cancel')
            elif p[0]=='settings': await self.settings(uid)
            elif p[0]=='notify':
                self.db.run('UPDATE users SET notifications=1-notifications WHERE id=?',(uid,))
                await self.settings(uid)
        except (ValueError,IndexError,OverflowError):
            await self.say(uid,'Bu tugma eskirgan. /start orqali qayta kiring.')

    async def admin_action(self,uid,p):
        action=p[0]
        permissions={
            'channels':'channels','channel':'channels','channel_add':'channels','channel_toggle':'channels','channel_delete':'channels','channel_delete_yes':'channels',
            'admins':'admins','admin_add':'admins','admin_remove':'admins',
            'broadcast':'broadcast','broadcast_send':'broadcast','stats':'stats','block':'block','backup':'backup',
            'movie_stats':'stats',
        }
        if not self.permit(uid,permissions.get(action,'catalog')):
            await self.say(uid,'Bu amal uchun huquqingiz yo‘q.'); return
        if action!='broadcast_send':
            self.db.set_state(uid,None)
        if action=='add':
            self.db.set_state(uid,{'type':'new','permission':'catalog','step':0,'data':{}})
            await self.prompt_field(uid,WIZARD[0])
        elif action=='lookup':
            self.db.set_state(uid,{'type':'lookup','permission':'catalog'})
            await self.say(uid,'Boshqarish uchun kino kodini yuboring. /cancel — bekor qilish.')
        elif action=='manage': await self.managed_movies(uid,max(0,int(p[1])))
        elif action=='movie': await self.managed_movie(uid,int(p[1]))
        elif action=='edit':
            mid=int(p[1])
            if not self.db.movie(mid,False): return
            rows=[[(label,f'a:field:{mid}:{field}')] for field,label in FIELDS.items() if field!='kind']
            rows.append([('⬅️ Kino',f'a:movie:{mid}')])
            await self.say(uid,'O‘zgartiriladigan maydon:',rows)
        elif action=='field':
            mid,field=int(p[1]),p[2]
            if field not in FIELDS or field=='kind' or not self.db.movie(mid,False): return
            self.db.set_state(uid,{'type':'edit','permission':'catalog','mid':mid,'field':field})
            await self.prompt_field(uid,field)
        elif action=='hide':
            mid=int(p[1])
            self.db.run('UPDATE movies SET hidden=1-hidden WHERE id=?',(mid,))
            await self.managed_movie(uid,mid)
        elif action=='delete':
            mid=int(p[1])
            await self.say(uid,'Kinoni barcha video variantlari bilan o‘chirasizmi?',[
                [('🗑 Ha, o‘chirish',f'a:delete_yes:{mid}'),('Bekor qilish',f'a:movie:{mid}')]])
        elif action=='delete_yes':
            self.db.run('DELETE FROM movies WHERE id=?',(int(p[1]),))
            await self.say(uid,'Kino o‘chirildi.'); await self.managed_movies(uid,0)
        elif action=='asset': await self.begin_asset(uid,int(p[1]))
        elif action=='assets':
            mid,page=int(p[1]),max(0,int(p[2]))
            data=self.db.all('SELECT * FROM assets WHERE movie_id=? ORDER BY season,episode,id LIMIT ? OFFSET ?',(mid,PAGE+1,page*PAGE))
            rows=[[(f"{r['season']}/{r['episode']} · {r['language']} · {r['quality']}",f"a:asset_menu:{r['id']}")] for r in data[:PAGE]]
            self.pager(rows,page,len(data)>PAGE,f'a:assets:{mid}:')
            rows.append([('⬅️ Kino',f'a:movie:{mid}')])
            await self.say(uid,'Video variantlari:',rows)
        elif action=='asset_menu':
            aid=int(p[1]); asset=self.db.one('SELECT * FROM assets WHERE id=?',(aid,))
            if not asset: return
            await self.say(uid,f"{asset['language']} · {asset['quality']}",[
                [('🔄 Videoni almashtirish',f'a:asset_replace:{aid}')],
                [('🗑 Variantni o‘chirish',f'a:asset_delete:{aid}')],
                [('⬅️ Kino',f"a:movie:{asset['movie_id']}")]])
        elif action=='asset_replace':
            self.db.set_state(uid,{'type':'replace','permission':'catalog','aid':int(p[1])})
            await self.say(uid,'Yangi video yoki video faylini yuboring. /cancel — bekor qilish.')
        elif action=='asset_delete':
            await self.say(uid,'Ushbu video variantini o‘chirasizmi?',[[('Ha, o‘chirish',f'a:asset_delete_yes:{int(p[1])}'),('Bekor qilish','a:manage:0')]])
        elif action=='asset_delete_yes':
            self.db.run('DELETE FROM assets WHERE id=?',(int(p[1]),)); await self.say(uid,'Video varianti o‘chirildi.')
        elif action=='channels': await self.channel_menu(uid,max(0,int(p[1])) if len(p)>1 else 0)
        elif action=='channel_add':
            self.db.set_state(uid,{'type':'channel','permission':'channels'})
            await self.say(uid,'Botni kanalga admin qiling. So‘ng 3 qatorda yuboring:\n\n@kanal_username yoki -100... ID\nhttps://t.me/kanal yoki taklif havolasi\nKanal nomi\n\nSaqlangach, obuna/import rejimini tugmadan yoqing.')
        elif action=='channel':
            ch=self.db.one('SELECT * FROM channels WHERE id=?',(int(p[1]),))
            if not ch: return
            await self.say(uid,ch['title'],[
                [('Majburiy obuna: '+('✅' if ch['required'] else '❌'),f"a:channel_toggle:{ch['id']}:required")],
                [('Avtoimport: '+('✅' if ch['source'] else '❌'),f"a:channel_toggle:{ch['id']}:source")],
                [('🗑 O‘chirish',f"a:channel_delete:{ch['id']}")],[('⬅️ Kanallar','a:channels')]])
        elif action=='channel_toggle':
            cid,field=int(p[1]),p[2]
            if field not in ('required','source'): return
            self.db.run(f'UPDATE channels SET {field}=1-{field} WHERE id=?',(cid,))
            await self.admin_action(uid,['channel',str(cid)])
        elif action=='channel_delete':
            await self.say(uid,'Kanalni obuna va import ro‘yxatidan o‘chirasizmi?',
                           [[('Ha, o‘chirish',f'a:channel_delete_yes:{int(p[1])}'),('Bekor qilish','a:channels')]])
        elif action=='channel_delete_yes':
            self.db.run('DELETE FROM channels WHERE id=?',(int(p[1]),)); await self.channel_menu(uid)
        elif action=='admins':
            page=max(0,int(p[1])) if len(p)>1 else 0
            admins=self.db.all('SELECT * FROM admins ORDER BY user_id LIMIT ? OFFSET ?',(PAGE+1,page*PAGE))
            rows=[[(f"{a['user_id']} · {a['role']} — olib tashlash",f"a:admin_remove:{a['user_id']}")] for a in admins[:PAGE]]
            self.pager(rows,page,len(admins)>PAGE,'a:admins:')
            rows += [[('➕ Qo‘shish / rolni o‘zgartirish','a:admin_add')],[('⬅️ Panel','admin')]]
            await self.say(uid,'Egalari: '+', '.join(map(str,sorted(self.owners)))+'\nRollar: manager yoki editor.',rows)
        elif action=='admin_add':
            self.db.set_state(uid,{'type':'admin','permission':'admins'})
            await self.say(uid,'Telegram ID va rolni yuboring:\n123456789 editor\nyoki\n123456789 manager\n\nEga huquqi faqat OWNER_IDS orqali beriladi.')
        elif action=='admin_remove':
            target=int(p[1]); self.db.run('DELETE FROM admins WHERE user_id=?',(target,))
            self.db.set_state(target,None)
            await self.admin_action(uid,['admins'])
        elif action=='block':
            self.db.set_state(uid,{'type':'block','permission':'block'})
            await self.say(uid,'Bloklash: 123456789 block\nBlokdan olish: 123456789 unblock')
        elif action=='stats': await self.stats(uid)
        elif action=='movie_stats': await self.stats(uid,int(p[1]))
        elif action=='broadcast':
            self.db.set_state(uid,{'type':'broadcast','permission':'broadcast'})
            await self.say(uid,'Tarqatiladigan bitta matn, rasm yoki videoni yuboring. Keyin tasdiqlash tugmasi chiqadi. /cancel — bekor qilish.')
        elif action=='broadcast_send':
            state=self.db.state(uid)
            if not state or state['type']!='broadcast_confirm' or len(p)<2 or str(state['message_id'])!=p[1]:
                await self.say(uid,'Bu e’lon allaqachon tasdiqlangan yoki bekor qilingan.'); return
            jid=self.db.broadcast(state['chat_id'],state['message_id'],uid)
            self.db.set_state(uid,None)
            n=self.db.one('SELECT COUNT(*) n FROM queue WHERE job_id=?',(jid,))['n']
            await self.say(uid,f'E’lon #{jid}: {n} foydalanuvchiga navbatga qo‘yildi. Holati statistikada ko‘rinadi.')
        elif action=='backup':
            with tempfile.TemporaryDirectory() as directory:
                path=str(Path(directory)/'kino-backup.db')
                # Independent connection in a worker avoids blocking the event loop.
                await asyncio.to_thread(backup_file,self.db.path,path)
                await self.bot.send_document(uid,FSInputFile(path),caption='Katalog va sozlamalar zaxirasi. Tiklash: README.md.')
        elif action=='resolve_report':
            self.db.run('UPDATE reports SET resolved=1 WHERE id=?',(int(p[1]),))
            await self.say(uid,'Xatolik hal qilingan deb belgilandi.',[[('⬅️ Xatoliklar','a:reports:0')]])
        elif action=='reports':
            page=max(0,int(p[1]))
            data=self.db.all('SELECT r.*,m.code FROM reports r LEFT JOIN movies m ON m.id=r.movie_id WHERE r.resolved=0 ORDER BY r.id DESC LIMIT ? OFFSET ?',(PAGE+1,page*PAGE))
            rows=[]
            for r in data[:PAGE]:
                await self.bot.send_message(uid,f"Xatolik #{r['id']} · {r['created']}\nUser ID: {r['user_id']}\nKino kodi: {r['code'] or 'o‘chirilgan'}\n{r['text']}")
                rows.append([(f"✅ #{r['id']} — hal qilindi",f"a:resolve_report:{r['id']}")])
            self.pager(rows,page,len(data)>PAGE,'a:reports:')
            rows.append([('⬅️ Admin','admin')])
            await self.say(uid,'Xatoliklar ro‘yxati.' if data else 'Xatoliklar yo‘q.',rows)

    @staticmethod
    def media(m):
        if m.video: return {'file_id':m.video.file_id,'media_type':'video'}
        if m.document and (m.document.mime_type or '').startswith('video/'):
            return {'file_id':m.document.file_id,'media_type':'document'}
        raise ValueError('Video yoki video turidagi fayl yuboring.')

    async def message(self,m,keyboard_input=True):
        uid=m.from_user.id
        if keyboard_input and m.text:
            keyboard=self.db.keyboard(uid)
            action=(keyboard or {}).get('actions',{}).get(m.text.strip())
            if action:
                if action.startswith('https://'):
                    # Reply keyboard buttons send text; URL opens from this message.
                    await self.bot.send_message(uid,f'{m.text.strip()}\n{action}')
                else:
                    await self.dispatch_action(uid,action)
                return
        state=self.db.state(uid)
        if not state:
            if m.text and m.text.startswith('/'):
                await self.say(uid,'Noma’lum buyruq. /help orqali yordamni oching.')
            elif m.text: await self.open_code(uid,m.text)
            else: await self.say(uid,'Kino kodini yuboring. Admin bo‘lsangiz: /admin')
            return
        if state.get('permission') and not self.permit(uid,state['permission']):
            self.db.set_state(uid,None); await self.say(uid,'Bu amal uchun huquqingiz yo‘q.'); return
        text=(m.text or '').strip()
        kind=state['type']
        try:
            if kind=='lookup':
                movie=self.db.by_code(code_value(text),public=False)
                if not movie: raise ValueError('Bu kod topilmadi. Boshqa kod kiriting.')
                self.db.set_state(uid,None)
                await self.managed_movie(uid,movie['id'])
            elif kind=='new':
                field=WIZARD[state['step']]
                value=m.photo[-1].file_id if field=='poster' and m.photo else validate(field,text)
                if field=='code' and self.db.by_code(value,False):
                    raise ValueError('Bu kod band. Boshqa kod kiriting.')
                state['data'][field]=value
                state['step']+=1
                if state['step']==len(WIZARD): await self.begin_asset(uid,None,state['data'])
                else:
                    self.db.set_state(uid,state); await self.prompt_field(uid,WIZARD[state['step']])
            elif kind=='asset':
                if state['step']<len(state['fields']):
                    field=state['fields'][state['step']]
                    state['data'][field]=validate(field,text); state['step']+=1
                    self.db.set_state(uid,state)
                    if state['step']<len(state['fields']): await self.prompt_field(uid,state['fields'][state['step']])
                    else: await self.say(uid,'Endi video yoki video faylini yuboring. /cancel — bekor qilish.')
                else:
                    data=state['data'] | self.media(m)
                    if state.get('draft'):
                        mid=self.db.create_with_asset(state['draft'],data)
                    else:
                        mid=state['mid']
                        self.db.add_asset(mid,data)
                    self.db.set_state(uid,None)
                    await self.say(uid,'Kino/video saqlandi.'); await self.managed_movie(uid,mid)
            elif kind=='edit':
                field=state['field']
                value=m.photo[-1].file_id if field=='poster' and m.photo else validate(field,text)
                if not self.db.movie(state['mid'],False):
                    self.db.set_state(uid,None); raise ValueError('Kino o‘chirilgan.')
                self.db.run(f'UPDATE movies SET {field}=? WHERE id=?',(value,state['mid']))
                self.db.set_state(uid,None); await self.managed_movie(uid,state['mid'])
            elif kind=='replace':
                media=self.media(m)
                if not self.db.one('SELECT id FROM assets WHERE id=?',(state['aid'],)):
                    self.db.set_state(uid,None)
                    raise ValueError('Bu video o‘chirilgan. Katalogdan qayta tanlang.')
                self.db.run('UPDATE assets SET file_id=?,media_type=? WHERE id=?',(media['file_id'],media['media_type'],state['aid']))
                self.db.set_state(uid,None); await self.say(uid,'Video yangilandi.')
            elif kind=='channel':
                lines=text.splitlines()
                if len(lines)!=3: raise ValueError('3 qatorda yuboring: kanal ID/username, havola, nom.')
                identifier=lines[0].strip()
                chat=await self.bot.get_chat(int(identifier) if identifier.lstrip('-').isdigit() else identifier)
                if chat.type!='channel': raise ValueError('Faqat Telegram kanalini qo‘shing.')
                membership=await self.bot.get_chat_member(chat.id,self.bot.id)
                if getattr(membership.status,'value',membership.status) not in ('administrator','creator'):
                    raise ValueError('Avval botni kanalga administrator qiling.')
                url=https_url(lines[1].strip())
                from urllib.parse import urlsplit
                if urlsplit(url).hostname!='t.me': raise ValueError('Kanal uchun https://t.me/... havola kiriting.')
                title=validate('title',lines[2])
                self.db.run('INSERT INTO channels(chat_id,title,url) VALUES(?,?,?) ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title,url=excluded.url',(chat.id,title,url))
                self.db.set_state(uid,None); await self.channel_menu(uid)
            elif kind=='admin':
                parts=text.split()
                if len(parts)!=2 or parts[1] not in ('manager','editor'):
                    raise ValueError('Misol: 123456789 editor yoki 123456789 manager')
                target=telegram_id(parts[0])
                if target in self.owners: raise ValueError('Eganing roli .env orqali belgilanadi.')
                self.db.run('INSERT INTO admins VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET role=excluded.role',(target,parts[1]))
                self.db.set_state(uid,None); await self.admin_action(uid,['admins'])
            elif kind=='block':
                parts=text.split()
                if len(parts)!=2 or not parts[0].isdigit() or parts[1] not in ('block','unblock'):
                    raise ValueError('Misol: 123456789 block yoki 123456789 unblock')
                target=telegram_id(parts[0])
                if self.role(target): raise ValueError('Administratorni bloklashdan avval uning rolini olib tashlang.')
                if not self.db.one('SELECT id FROM users WHERE id=?',(target,)): raise ValueError('Bu foydalanuvchi botni hali ochmagan.')
                self.db.run('UPDATE users SET blocked=? WHERE id=?',(int(parts[1]=='block'),target))
                self.db.set_state(uid,None); await self.say(uid,'Foydalanuvchi holati yangilandi.')
            elif kind=='report':
                if not text or len(text)>1000: raise ValueError('1–1000 belgili matn yuboring.')
                if not self.db.movie(state['mid'],False):
                    self.db.set_state(uid,None); raise ValueError('Bu kino o‘chirilgan.')
                self.db.run('INSERT INTO reports(user_id,movie_id,text,created) VALUES(?,?,?,?)',(uid,state['mid'],text,self.db.now()))
                self.db.set_state(uid,None); await self.say(uid,'Xabaringiz admin panelga yuborildi.')
            elif kind=='broadcast':
                if not (m.text or m.photo or m.video or m.document or m.animation): raise ValueError('Matn, rasm, video yoki fayl yuboring.')
                if m.media_group_id: raise ValueError('Albom o‘rniga bitta xabar yuboring.')
                self.db.set_state(uid,{'type':'broadcast_confirm','permission':'broadcast','chat_id':m.chat.id,'message_id':m.message_id})
                await self.bot.copy_message(uid,m.chat.id,m.message_id)
                await self.say(uid,'Yuqoridagi e’lonni xabarnomalari yoqilgan foydalanuvchilarga yuborilsinmi?',
                               [[('📨 Yuborish',f'a:broadcast_send:{m.message_id}'),('Bekor qilish','admin')]])
            elif kind=='broadcast_confirm': await self.say(uid,'Tasdiqlash tugmasini bosing yoki /cancel yozing.')
        except psycopg2.IntegrityError:
            await self.say(uid,'Bu kod yoki shu til/sifat/fasl/qismdagi video allaqachon mavjud. Boshqa qiymat kiriting yoki /cancel bilan chiqib, mavjud videoni tahrirlang.')
        except (ValueError,OverflowError) as e:
            await self.say(uid,str(e) if isinstance(e,ValueError) else 'Raqam juda katta. To‘g‘ri qiymat kiriting.')
        except TelegramAPIError:
            await self.say(uid,'Telegram amalni bajarmadi. Kanal, bot huquqlari yoki yuborilgan faylni tekshiring.')

    async def channel_post(self,m):
        if not self.db.one('SELECT id FROM channels WHERE chat_id=? AND source=1',(m.chat.id,)):
            return
        try:
            data=parse_channel_caption(m.caption or m.text)
            if data is None: return
            data.update(self.media(m))
            mid=self.db.import_post(m.chat.id,m.message_id,data)
            if mid:
                await self.owner_notice(f"Avtoimport: {data['title']} · {data['code']} saqlandi.")
        except (ValueError,psycopg2.IntegrityError) as e:
            await self.owner_notice(f'Avtoimport xatosi: kanal {m.chat.id}, post {m.message_id}.\n{e}')

    async def owner_notice(self,text):
        for owner in self.owners:
            try: await self.bot.send_message(owner,text)
            except TelegramAPIError: pass

    async def error(self,event:ErrorEvent):
        # Stack locations aid diagnosis; exception text may contain private URLs.
        log.error('Update failed: %s\n%s',type(event.exception).__name__,
                  ''.join(traceback.format_tb(event.exception.__traceback__)))
        update=event.update
        uid=update.message.from_user.id if update.message and update.message.from_user else (
            update.callback_query.from_user.id if update.callback_query else None)
        if uid:
            with contextlib.suppress(TelegramAPIError):
                await self.say(uid,'Amal bajarilmadi. /start orqali qayta urinib ko‘ring.')
        return True

    async def queue_item(self,row):
        payload=json.loads(row['payload']); uid=row['user_id']
        user=self.db.one('SELECT * FROM users WHERE id=?',(uid,))
        if not user or user['blocked'] or not user['reachable'] or not user['notifications']:
            return 'skipped'
        if row['kind']=='broadcast':
            if not self.permit(payload['creator'],'broadcast'): return 'skipped'
            await self.bot.copy_message(uid,payload['chat_id'],payload['message_id'])
        else:
            movie=self.db.movie(payload['movie_id'])
            if not movie or not self.db.one("SELECT 1 FROM marks WHERE user_id=? AND movie_id=? AND kind='follow'",(uid,payload['movie_id'])):
                return 'skipped'
            if not self.db.one('SELECT id FROM assets WHERE movie_id=? AND season=? AND episode=?',
                               (movie['id'],payload['season'],payload['episode'])): return 'skipped'
            # Background notifications must not replace a user's active keyboard.
            await self.bot.send_message(uid,
                f"{movie['title']}: {payload['season']}-fasl, {payload['episode']}-qism qo‘shildi.\n"
                f"https://t.me/{self.username}?start=film_{movie['code']}")
        return 'sent'

    async def queue_worker(self):
        # Single process/worker. A restart replays the last in-flight item at most once.
        self.db.run("UPDATE queue SET state='pending' WHERE state='sending'")
        while True:
            row=self.db.one("SELECT q.*,j.kind,j.payload FROM queue q JOIN jobs j ON j.id=q.job_id WHERE q.state='pending' AND q.next_at<=? ORDER BY q.id LIMIT 1",(time.time(),))
            if not row:
                await asyncio.sleep(1); continue
            self.db.run("UPDATE queue SET state='sending' WHERE id=?",(row['id'],))
            try:
                state=await self.queue_item(row)
                self.db.run('UPDATE queue SET state=? WHERE id=?',(state,row['id']))
            except TelegramRetryAfter as e:
                self.db.run("UPDATE queue SET state='pending',next_at=? WHERE id=?",(time.time()+e.retry_after+1,row['id']))
                await asyncio.sleep(e.retry_after+1)
            except TelegramForbiddenError:
                self.db.run('UPDATE users SET reachable=0 WHERE id=?',(row['user_id'],))
                self.db.run("UPDATE queue SET state='failed' WHERE id=?",(row['id'],))
            except (TelegramAPIError,OSError):
                attempts=row['attempts']+1
                self.db.run('UPDATE queue SET state=?,attempts=?,next_at=? WHERE id=?',
                            ('failed' if attempts>=5 else 'pending',attempts,time.time()+min(300,2**attempts),row['id']))
            except Exception:
                log.exception('Queue item failed id=%s',row['id'])
                self.db.run("UPDATE queue SET state='failed' WHERE id=?",(row['id'],))
            await asyncio.sleep(.08)

def backup_file(source,destination):
    with sqlite3.connect(source) as src,sqlite3.connect(destination) as dst:
        src.backup(dst)

async def main():
    load_dotenv()
    token=os.getenv('BOT_TOKEN','').strip()
    if not token:
        raise SystemExit('BOT_TOKEN kiritilmagan. .env.example ni .env ga nusxalab to‘ldiring.')
    try:
        owners={telegram_id(x) for x in os.getenv('OWNER_IDS','').split(',') if x.strip()}
        if not owners or min(owners)<1: raise ValueError
    except ValueError:
        raise SystemExit('OWNER_IDS ichiga o‘zingizning raqamli Telegram ID raqamingizni yozing.')
    dsn = os.getenv('DATABASE_URL', '').strip()
    if not dsn:
        raise SystemExit('DATABASE_URL kiritilmagan. Supabase connection string ni .env ga yozing.')
    # Local file lock still prevents two polling processes on same host.
    lock_path = os.getenv('INSTANCE_LOCK', 'data/bot.lock')
    with InstanceLock(lock_path), contextlib.closing(Store(dsn, os.getenv('TIMEZONE', 'Asia/Tashkent'))) as db:
        async with Bot(token) as bot:
            await run_bot(bot, db, owners)

async def run_bot(bot,db,owners):
    me=await bot.get_me()
    app=App(bot,db,owners,os.getenv('BOT_TITLE','Kino Bot'))
    app.username=me.username
    webhook=await bot.get_webhook_info()
    if webhook.url:
        raise SystemExit('Botda webhook faol. Avval oldingi xizmatni to‘xtatib, webhookni olib tashlang.')
    await bot.set_my_commands([BotCommand(command='start',description='Bosh menyu'),
                              BotCommand(command='help',description='Yordam'),
                              BotCommand(command='admin',description='Admin panel'),
                              BotCommand(command='id',description='Telegram ID'),
                              BotCommand(command='cancel',description='Amalni bekor qilish')])
    worker=asyncio.create_task(app.queue_worker(),name='delivery-queue')
    polling=asyncio.create_task(app.dp.start_polling(bot,
        allowed_updates=['message','callback_query','channel_post'],
        close_bot_session=False,tasks_concurrency_limit=64),name='telegram-polling')
    log.info('Bot started: @%s',me.username)
    try:
        done,_=await asyncio.wait((worker,polling),return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()  # Let the host restart the bot if either service fails.
    finally:
        worker.cancel()
        if not polling.done():
            with contextlib.suppress(RuntimeError,asyncio.TimeoutError):
                await asyncio.wait_for(app.dp.stop_polling(),timeout=10)
        polling.cancel()
        await asyncio.gather(worker,polling,return_exceptions=True)

if __name__=='__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    asyncio.run(main())

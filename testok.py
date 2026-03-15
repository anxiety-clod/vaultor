return
                    
                seller_id, buyer_id, name = deal
                
                if buyer_id != uid:
                    await call.answer("❌ Только покупатель может оценить", show_alert=True)
                    return
                
                await add_rating(seller_id, uid, code, rating)
                stats = await get_seller_stats(seller_id)
                
                await call.message.edit(
                    f"⭐ <b>СПАСИБО ЗА ОЦЕНКУ!</b>\n\n"
                    f"Вы поставили {rating}⭐\n\n"
                    f"📊 Рейтинг продавца: {stats['rating']} ({stats['total_votes']} оценок)",
                    parse_mode=ParseMode.HTML
                )
                await call.answer("✅ Оценка сохранена", show_alert=False)
                
            except Exception as e:
                if "UNIQUE constraint failed" in str(e):
                    await call.answer("❌ Вы уже оценили эту сделку", show_alert=True)
                else:
                    print(f"Ошибка: {e}")
                    await call.answer("❌ Ошибка", show_alert=True)
            finally:
                app.processing["rate"].discard(key)
            
            return

        # ===== ОТКРЫТЬ СПОР =====
        if data.startswith("dispute_"):
            code = data.split("_")[1]
            
            # Защита от двойного нажатия
            if code in app.processing["dispute"]:
                return
            app.processing["dispute"].add(code)
            
            try:
                user_states[uid] = {"step": "dispute_msg", "deal_code": code}
                await call.message.reply(
                    "⚖ <b>СПОР</b>\n\nОпишите проблему:",
                    parse_mode=ParseMode.HTML
                )
                await call.answer()
            finally:
                app.processing["dispute"].discard(code)
            
            return

        # ===== РЕШЕНИЕ СПОРА АДМИНОМ =====
        if data.startswith("resolve_"):
            if uid != ADMIN_ID:
                await call.answer("❌ Доступ запрещен", show_alert=True)
                return
            
            parts = data.split("_")
            code = parts[1]
            choice = parts[2]
            key = f"{code}_{choice}"
            
            await call.answer("⚖ Обрабатываю решение...", show_alert=False)
            
            # Защита от двойного нажатия
            if key in app.processing["resolve"]:
                return
            app.processing["resolve"].add(key)
            
            try:
                async with aiosqlite.connect(DB) as db:
                    cur = await db.execute("SELECT buyer_id, seller_id, amount, name, status FROM deals WHERE code=?", (code,))
                    deal = await cur.fetchone()
                
                if not deal:
                    await call.answer("❌ Сделка не найдена", show_alert=True)
                    return
                    
                buyer_id, seller_id, amount, name, status = deal
                
                if status != "dispute":
                    await call.answer("❌ Спор уже решён", show_alert=True)
                    return
                
                if code in escrow:
                    amount = escrow.pop(code)["amount"]
                
                if choice == "buyer":
                    await change_balance(buyer_id, amount)
                    await app.send_message(buyer_id, f"💰 Спор решен в вашу пользу! +{amount:.2f} ₽")
                    await app.send_message(seller_id, f"❌ Спор проигран")
                else:
                    await change_balance(seller_id, amount)
                    await update_seller_deal_stats(seller_id, amount)
                    await app.send_message(seller_id, f"💰 Спор решен в вашу пользу! +{amount:.2f} ₽")
                    await app.send_message(buyer_id, f"❌ Спор проигран")
                
                async with aiosqlite.connect(DB) as db:
                    await db.

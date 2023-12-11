import time, uuid, datetime

from pandas import DataFrame
from typing import Optional, Dict, Any, Union
import requests
from tinkoff.invest.services import InstrumentsService
from tinkoff.invest import (
    Client,
    OrderDirection,
    OrderType
)

MAX_RETRIES = 100

# Функция для проверки, находится ли текущее время внутри интервала [start_time, end_time]
def is_within_time_interval(start_time, end_time):
    now = datetime.datetime.now().time()
    return start_time <= now <= end_time

# Функция для проверки, является ли текущий день недели будним (понедельник - пятница)
def is_weekday():
    now = datetime.datetime.now().weekday()
    return now >= 0 and now <= 4  # Понедельник - 0, Пятница - 4

# Получаем информацию об инструментах
def create_instruments_dataframe() -> DataFrame:

    with Client(TOKEN) as client:

        instruments: InstrumentsService = client.instruments

        data = []
        try:
            for method in ['shares', 'bonds', 'etfs', 'currencies', 'futures']:
                for item in getattr(instruments, method)().instruments:
                    data.append({
                        'ticker': item.ticker,
                        'figi': item.figi,
                        'type': method,
                        'name': item.name,
                        'lot_size': item.lot
                    })

        except requests.exceptions.RequestException as e:

            raise Exception(f"Ошибка запроса: {e}")

    return DataFrame(data)

def get_instrument_info(df: DataFrame, TICKER: Optional[str] = None, FIGI: Optional[str] = None) -> Union[str, Dict[str, Any]]:

    if TICKER:
        filtered_df = df[df['ticker'] == TICKER]
        if not filtered_df.empty:
            return {
                'ticker': filtered_df.iloc[0]['ticker'],
                'figi': filtered_df.iloc[0]['figi'],
                'name': filtered_df.iloc[0]['name'],
                'lot_size': filtered_df.iloc[0]['lot_size']
            }
        else:
            print(f"Тикер {TICKER} не найден.")
            time.sleep(300)

    elif FIGI:
        filtered_df = df[df['figi'] == FIGI]
        if not filtered_df.empty:
            return {
                'ticker': filtered_df.iloc[0]['ticker'],
                'figi': filtered_df.iloc[0]['figi'],
                'name': filtered_df.iloc[0]['name'],
                'lot_size': filtered_df.iloc[0]['lot_size']
            }
        else:
            print(f"FIGI {FIGI} не найден.")
            time.sleep(300)
            
def user_input_token():

    while True:

        TOKEN = input("Введите ваш токен Tinkoff API: ")

        try:
            with Client(TOKEN) as client:
                Account_id = client.users.get_accounts().accounts[0].id
                print("Account_id:", Account_id)
                print("----------------------------------------------")
                return TOKEN, Account_id
        except Exception as e:
            print("ОШИБКА ТОКЕНА:", str(e))
            print("----------------------------------------------")
            print("Пожалуйста, проверьте правильность введенного токена и попробуйте еще раз.")
            time.sleep(0.1)
            print("----------------------------------------------")
    
def user_input_tiker(df):

    while True:

        tiker_number = input("Выберите компанию:\n1 - Сбербанк\n2 - Татнефть\n3 - Ростелеком\n")

        # Обрабатываем выбор пользователя
        if tiker_number == '1':

            print("Вы выбрали Сбербанк. Запрашиваю информацию о портфеле...")
            info_Ob = get_instrument_info(df, TICKER="SBER")
            info_Pref = get_instrument_info(df, TICKER="SBERP")

            figi_Ob = info_Ob.get('figi', 'Информация не найдена')
            figi_Pref = info_Pref.get('figi', 'Информация не найдена')

            return figi_Ob, figi_Pref

        elif tiker_number == '2':

            print("Вы выбрали Татнефть. Запрашиваю информацию о портфеле...")
            info_Ob = get_instrument_info(df, TICKER="TATN")
            info_Pref = get_instrument_info(df, TICKER="TATNP")

            figi_Ob = info_Ob.get('figi', 'Информация не найдена')
            figi_Pref = info_Pref.get('figi', 'Информация не найдена')

            return figi_Ob, figi_Pref

        elif tiker_number == '3':

            print("Вы выбрали Ростелеком. Запрашиваю информацию о портфеле...")
            info_Ob = get_instrument_info(df, TICKER="RTKM")
            info_Pref = get_instrument_info(df, TICKER="RTKMP")

            figi_Ob = info_Ob.get('figi', 'Информация не найдена')
            figi_Pref = info_Pref.get('figi', 'Информация не найдена')

            return figi_Ob, figi_Pref

        else:

            print("Некорректный выбор. Пожалуйста, выберите 1, 2 или 3.")
            time.sleep(0.1)
            print("Попробуйте еще раз.")

def user_input_spread():
    
    while True:
        
        price_1 = float(input("Введите величину спреда при которой будут проданы префа и куплена обычка "))
        price_2 = float(input("Введите величину спреда при которой будет продана обычка и куплены префа "))

        if price_1 < price_2:
            return price_1, price_2
        else:
            time.sleep(5)
            print("Некорректные значения. Первое число должно быть меньше второго")
            
# Функция показывающая кол-во обычки и префов в портфеле и состав портфеля
def get_portfolio_info(TOKEN, df):

    with Client(TOKEN) as client:

        retries = 0

        while retries < MAX_RETRIES:
            try:

                PositionResponse = client.operations.get_positions(account_id=Account_id)

                # Извлекаем информацию о деньгах
                money_info = PositionResponse.money
                for money in money_info:
                    currency = money.currency
                    units = money.units
                    print(f"Денег: {units} {currency}")

                # Извлекаем информацию о ценных бумагах
                securities_info = PositionResponse.securities
                for security in securities_info:
                    figi = security.figi
                    balance = security.balance
                    # Используем функцию get_instrument_info для получения тикера и имени по FIGI
                    instrument_info = get_instrument_info(df, FIGI=figi)
                    if isinstance(instrument_info, dict):
                        print(f"Акции: Тикер: Название: '{instrument_info['name']}', '{instrument_info['ticker']}' - {balance} шт.")
                    else:
                        print(instrument_info)
                break

            except Exception as e:

                print(f"Ошибка при получении информации о портфеле: {e}")
                print("----------------------------------------------")
                retries += 1
                time.sleep(10)  # Подождать перед повторным запросом
        else:

            print("Не удалось получить информацию о портфеле после", MAX_RETRIES, "попыток.")
            print("----------------------------------------------")

        Securities = PositionResponse.securities

        KObich = 0
        KPrefa = 0

        for security in Securities:
            if security.figi == figi_Pref:
                KPrefa = security.balance
            if security.figi == figi_Ob:
                KObich = security.balance

        return KPrefa, KObich, Securities, PositionResponse

# Функция для вычисления последней цены
def get_last_prices(TOKEN, figi_Ob, figi_Pref):
    
    with Client(TOKEN) as client:
        # Если произошла ошибка "Stream removed" или другие ошибки, связанные с соединением, можно попробовать повторить запрос через некоторый промежуток времени.
        retries = 0
        while retries < MAX_RETRIES:
            try:
                LastPriceObich = client.market_data.get_order_book(figi=figi_Ob, depth=5).last_price
                LastPricePrefa = client.market_data.get_order_book(figi=figi_Pref, depth=5).last_price
                break
            except Exception as e:
                print(f"Ошибка при получении цены акции: {e}")
                retries += 1
                time.sleep(10)  # Подождать перед повторным запросом
        else:
            print("Не удалось получить цену акции после", MAX_RETRIES, "попыток.")

        LastPrice_Ob = LastPriceObich.units + LastPriceObich.nano * 1e-9
        LastPrice_Pref = LastPricePrefa.units + LastPricePrefa.nano * 1e-9

        spread = round(LastPrice_Ob - LastPrice_Pref, 2)

        return spread, LastPrice_Ob, LastPrice_Pref

# Функция покупки/продажи
def trade (TOKEN, Order, Account_id, KolVo, FigiStock, OrderDirection):

    with Client(TOKEN) as client:
        # Если произошла ошибка "Stream removed" или другие ошибки, связанные с соединением, можно попробовать повторить запрос через некоторый промежуток времени.
        retries = 0
        while retries < MAX_RETRIES:
            try:
                client.orders.post_order(                           # Создаем заявку
                order_id=Order,                                     # id заявки - текущее время
                figi=FigiStock,                                     # Бумага
                quantity=KolVo,                                     # Для бумаг где в лоте 1 шт.
                account_id=Account_id,
                direction=OrderDirection,                           # Заявка на покупку или продажу
                order_type=OrderType.ORDER_TYPE_BESTPRICE           # По лучшей цене
                )
                if OrderDirection == OrderDirection.ORDER_DIRECTION_BUY: 
                    print('Купили', KolVo, 'шт.')                   # Для бумаг где в лоте 1 шт.
                else: 
                    print('Продали', KolVo, 'шт.')                  # Для бумаг где в лоте 1 шт.
                time.sleep(10)                                      # Даем время на покупку/продажу
                break                                               # Выходим из цикла, если выставили заявку
            except Exception as e:  
                print(f"Ошибка при создании ордера: {e}")
                retries += 1
                time.sleep(10)  # Подождать перед повторным запросом
        else:
            print("Не удалось создать ордер после", MAX_RETRIES,"попыток.")

if __name__ == '__main__':

    print("---------------------------------------------")
    print('Скрипт торговли по спреду обычка/преф запущен')
    print("---------------------------------------------")
    TOKEN, Account_id = user_input_token()
    print('Запрашиваю информацию об инструментах...')
    df = create_instruments_dataframe()
    print("---------------------------------------------")
    figi_Ob, figi_Pref = user_input_tiker(df)
    print("---------------------------------------------")
    print('Портфель:')
    KPrefa, KObich, Securities, PositionResponse = get_portfolio_info(TOKEN, df)
    print("---------------------------------------------")
    price_1, price_2 = user_input_spread()
    print("---------------------------------------------")

    while True:

            try:

                if is_weekday() and (is_within_time_interval(datetime.time(10, 0), datetime.time(18, 45)) or is_within_time_interval(datetime.time(19, 0), datetime.time(23, 59))):

                    spread, LastPrice_Ob, LastPrice_Pref = get_last_prices(TOKEN, figi_Ob, figi_Pref)
                    info_Ob = get_instrument_info(df, FIGI=figi_Ob)
                    info_Pref = get_instrument_info(df, FIGI=figi_Pref)
                    name_Ob = info_Ob.get('name', 'Неизвестное название')
                    name_Pref = info_Pref.get('name', 'Неизвестное название')

                    # Спред меньше price_1 покупаем обычку если ничего нет, или продаем префа и покупаем обычку
                    if spread < price_1:
                        
                        print ('Спред меньше 1 делаем соотношение 0% в обычке и 100% в префе') 
                        print ('Текущий спред', spread)
                        print ('Цена обычки =', LastPrice_Ob)
                        print ('Цена префов =', LastPrice_Pref)
                        print ('Дата:', datetime.datetime.now()) 
                        print('Портфель:')
                        KPrefa, KObich, Securities, PositionResponse = get_portfolio_info(TOKEN, df)

                        KolVo_Ob = 0
                        KolVo_Pref = 0
                        figi_Ob_found = 0
                        figi_Pref_found = 0

                        # Пробегаемся циклом по массиву акций
                        for security in Securities:
                            # Проверяем есть ли в портфеле обычка
                            if security.figi == figi_Ob:
                                KolVo_Ob = security.balance
                                figi_Ob_found = 1
                            elif security.figi == figi_Pref:
                                KolVo_Pref = security.balance
                                figi_Pref_found = 1

                        # Если у нас в портфеле префа мы их продаем и покупаем обычку
                        if figi_Pref_found > 0:
                            
                            # Совершаем продажу
                            print('У нас в портфеле префа', KolVo_Pref, 'шт. нужно их продать и купить обычку')
                            lot_size = info_Pref.get('lot_size', 'Информация о лотности не найдена')
                            KolVo_Pref = KolVo_Pref/lot_size 
                            trade (TOKEN, str(uuid.uuid4()), Account_id, KolVo_Pref, figi_Pref, OrderDirection.ORDER_DIRECTION_SELL)
                            
                            # Получаем информацию для покупки
                            spread, LastPrice_Ob, LastPrice_Pref = get_last_prices(TOKEN, figi_Ob, figi_Pref)
                            Money =  int(PositionResponse.money[0].units)
                            lot_size = info_Ob.get('lot_size', 'Информация о лотности не найдена')
                            KolVo = int(Money/LastPrice_Ob/lot_size*0.98)
                            trade (TOKEN, str(uuid.uuid4()), Account_id, KolVo, figi_Ob, OrderDirection.ORDER_DIRECTION_BUY)

                        if figi_Ob_found > 0:

                            print('У нас в портфеле обычки', KolVo_Ob, 'шт. Ждем роста обычки.')
                            
                        if figi_Ob_found == 0 and figi_Pref_found == 0:

                            print(f'В портфеле нет акций {name_Ob}, нужно купить обычку')

                            # Получаем информацию для покупки
                            spread, LastPrice_Ob, LastPrice_Pref = get_last_prices(TOKEN, figi_Ob, figi_Pref)
                            Money =  int(PositionResponse.money[0].units)
                            lot_size = info_Ob.get('lot_size', 'Информация о лотности не найдена')
                            KolVo = int(Money/LastPrice_Ob/lot_size*0.98)
                            trade (TOKEN, str(uuid.uuid4()), Account_id, KolVo, figi_Ob, OrderDirection.ORDER_DIRECTION_BUY)
                                
                        print("---------------------------------------------")

                    # Спред больше price_2 покупаем префа если ничего нет, или продаем обычку и покупаем префа
                    elif spread > price_2:
                        

                        print ('Спред меньше 1 делаем соотношение 0% в обычке и 100% в префе') 
                        print ('Текущий спред', spread)
                        print ('Цена обычки =', LastPrice_Ob)
                        print ('Цена префов =', LastPrice_Pref)
                        print ('Дата:', datetime.datetime.now()) 
                        print('Портфель:')
                        KPrefa, KObich, Securities, PositionResponse = get_portfolio_info(TOKEN, df)

                        KolVo_Ob = 0
                        KolVo_Pref = 0
                        figi_Ob_found = 0
                        figi_Pref_found = 0

                        # Пробегаемся циклом по массиву акций
                        for security in Securities:
                            # Проверяем есть ли в портфеле обычка
                            if security.figi == figi_Ob:
                                KolVo_Ob = security.balance
                                figi_Ob_found = 1
                            elif security.figi == figi_Pref:
                                KolVo_Pref = security.balance
                                figi_Pref_found = 1

                        # Если у нас в портфеле обычка мы ее продаем и покупаем префа
                        if figi_Ob_found > 0:

                            # Совершаем продажу
                            print('У нас в портфеле обычка', KolVo_Ob, 'шт. нужно ее продать и купить префа')
                            lot_size = info_Ob.get('lot_size', 'Информация о лотности не найдена')
                            KolVo_Ob = KolVo_Ob/lot_size
                            trade (TOKEN, str(uuid.uuid4()), Account_id, KolVo_Ob, figi_Ob, OrderDirection.ORDER_DIRECTION_SELL)

                            # Получаем информацию для покупки
                            spread, LastPrice_Ob, LastPrice_Pref = get_last_prices(TOKEN, figi_Ob, figi_Pref)
                            Money =  int(PositionResponse.money[0].units)
                            lot_size = figi_Pref.get('lot_size', 'Информация о лотности не найдена')
                            KolVo = int(Money/LastPrice_Pref/lot_size*0.98)
                            trade (TOKEN, str(uuid.uuid4()), Account_id, KolVo, figi_Pref, OrderDirection.ORDER_DIRECTION_BUY)

                        if figi_Pref_found > 0:

                            print('У нас в портфеле префов', KolVo_Pref, 'шт. Ждем роста префов.')

                        if figi_Ob_found == 0 and figi_Pref_found == 0:

                            print(f'В портфеле нет акций {name_Pref}, нужно купить обычку')

                            # Получаем информацию для покупки
                            spread, LastPrice_Ob, LastPrice_Pref = get_last_prices(TOKEN, figi_Ob, figi_Pref)
                            Money =  int(PositionResponse.money[0].units)
                            lot_size = figi_Pref.get('lot_size', 'Информация о лотности не найдена')
                            KolVo = int(Money/LastPrice_Pref/lot_size*0.98)
                            trade (TOKEN, str(uuid.uuid4()), Account_id, KolVo, figi_Pref, OrderDirection.ORDER_DIRECTION_BUY)

                        print("---------------------------------------------")
                        
                    else:

                        print ('Текущий спред', spread)
                        print ('Цена обычки =', LastPrice_Ob)
                        print ('Цена префов =', LastPrice_Pref)
                        print ('Дата:', datetime.datetime.now()) 
                        print('Портфель:')
                        KPrefa, KObich, Securities, PositionResponse = get_portfolio_info(TOKEN, df)
                        print("---------------------------------------------")
                        
                    time.sleep(30)

                else:

                    print('Биржа не работает ')
                    time.sleep(300)

            except Exception as e:

                print("Произошла ошибка:", str(e))
                print("------------------------------------------")

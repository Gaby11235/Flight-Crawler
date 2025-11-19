from bs4 import BeautifulSoup
from DrissionPage import WebPage
import time
import logging
import csv
from datetime import date, datetime,timedelta
from db_operations import DatabaseManager
from tenacity import retry, stop_after_attempt, wait_exponential
from collections import deque

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_airline_plane_no(flight_div):
    """
    获取航班号，支持中转航班（多个航班号）
    返回格式：
    - 单个航班：直接返回航班号
    - 多个航班：航班号1 + 航班号2 + ...（用 + 连接）
    """
    # 查找所有具有 class "plane-No" 的 <span> 标签
    plane_no_spans = flight_div.find_all('span', class_='plane-No')

    if not plane_no_spans:
        return "N/A"

    # 获取所有航班号
    plane_nos = []
    for span in plane_no_spans:
        # 获取航班号文本（去除机型信息）
        plane_no_text = span.get_text(strip=True)
        # 分割文本，只取航班号部分（去除机型信息）
        plane_no = plane_no_text.split()[0] if plane_no_text else "N/A"
        if plane_no != "N/A":
            plane_nos.append(plane_no)

    if not plane_nos:
        return "N/A"

    # 如果只有一个航班号，直接返回
    if len(plane_nos) == 1:
        return plane_nos[0]

    # 多个航班号用 + 连接
    return " + ".join(plane_nos)


def get_airline_name_divs(flight_div):
    return flight_div.find_all("div", {"class": "airline-name"})


def get_airline_name_first(flight_div):
    return flight_div.find("div", {"class": "airline-name"})


def get_departure_box(flight_div):
    return flight_div.find("div", {"class": "depart-box"})


def get_arrival_box(flight_div):
    return flight_div.find("div", {"class": "arrive-box"})


def get_departure_airport(flight_div):
    try:
        return get_departure_box(flight_div).find("div", {"class": "airport"}).get_text(strip=True)
    except AttributeError:
        return "N/A"


def get_departure_time(flight_div):
    try:
        return get_departure_box(flight_div).find("div", {"class": "time"}).get_text(strip=True)
    except AttributeError:
        return "N/A"


def get_arrival_airport(flight_div):
    try:
        return get_arrival_box(flight_div).find("div", {"class": "airport"}).get_text(strip=True)
    except AttributeError:
        return "N/A"


def get_arrive_time(flight_div):
    try:
        return get_arrival_box(flight_div).find("div", {"class": "time"}).get_text(strip=True)
    except AttributeError:
        return "N/A"


def get_flight_information(flight_div):
    try:
        return flight_div.find("div", {"class": "transfer-info-group"}).get_text(strip=True)
    except AttributeError:
        return "N/A"


def get_flight_price(flight_div):
    try:
        return flight_div.find("span", {"class": "price"}).get_text(strip=True)
    except AttributeError:
        return "N/A"


def revise_result(flight_div, airline_name, departure, arrival, departure_date):
    result = {
        'airline': 'null',
        'departure_airport': 'null',
        'arrival_airport': 'null',
        'departure_time': 'null',
        'arrival_time': 'null',
        'FlightInformation': 'null',
        'price': 'null',
        'plane_no': 'null',
        'search_departure': departure,
        'search_arrival': arrival,
        'search_departure_date': departure_date,
        'crawl_date':'null'
    }
    result['airline'] = airline_name
    result['departure_airport'] = get_departure_airport(flight_div)
    result['arrival_airport'] = get_arrival_airport(flight_div)
    result['departure_time'] = get_departure_time(flight_div)
    result['arrival_time'] = get_arrive_time(flight_div)
    result['FlightInformation'] = get_flight_information(flight_div)
    result['plane_no'] = get_airline_plane_no(flight_div)

    # 处理价格
    raw_price = get_flight_price(flight_div)
    if raw_price and raw_price != 'null':
        # 移除¥符号和逗号，转换为浮点数
        try:
            price_str = raw_price.replace('¥', '').replace(',', '')
            result['price'] = float(price_str)
        except (ValueError, TypeError):
            result['price'] = None
            logging.warning(f"价格转换失败: {raw_price}")
    else:
        result['price'] = None

    # 处理日期格式
    try:
        # 将爬取日期转换为MySQL日期格式
        result['crawl_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 将搜索日期转换为MySQL日期格式
        if departure_date:
            result['search_departure_date'] = datetime.strptime(departure_date, '%Y-%m-%d').strftime('%Y-%m-%d')
    except Exception as e:
        logging.error(f"日期转换失败: {str(e)}")
        result['crawl_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        result['search_departure_date'] = departure_date

    return result


def data_processing(flight_div, departure, arrival, departure_date):
    """
    处理航班数据，区别处理单个航班和多个航班
    """
    airline_divs = get_airline_name_divs(flight_div)

    # 获取所有航空公司名称
    airline_names = []
    for div in airline_divs:
        airline_name = div.get_text(strip=True)
        if airline_name:
            airline_names.append(airline_name)

    # 如果没有找到航空公司名称，尝试获取第一个
    if not airline_names:
        first_airline = get_airline_name_first(flight_div)
        if first_airline:
            airline_names = [first_airline.get_text(strip=True)]
        else:
            airline_names = ["N/A"]

    # 判断是否为中转航班
    is_transfer = len(airline_names) > 1

    # 根据航班类型处理航空公司名称
    if is_transfer:
        # 中转航班：用 + 连接多个航空公司名称
        airline_name = " + ".join(airline_names)
        logging.info(f"处理中转航班: {airline_name}")
    else:
        # 单个航班：直接使用航空公司名称
        airline_name = airline_names[0]
        logging.info(f"处理单个航班: {airline_name}")

    return revise_result(flight_div, airline_name, departure, arrival, departure_date)

def filter_target_airlines(flights):
    """
    过滤目标航空公司的航班，支持中转航班
    目标航司：东航(MU)、厦航(MF)、东海航(DZ)、国泰航空(CX)、大韩航空(KE)
    """
    target_airlines = {
        'MU': '东方航空',
        'MF': '厦门航空',
        'DZ': '东海航空',
        'CX': '国泰航空',
        'KE': '大韩航空'
    }

    filtered_flights = []
    for flight in flights:
        airline = flight['airline']
        plane_no = flight['plane_no']

        # 检查是否为中转航班
        if " + " in plane_no:
            # 中转航班：检查所有航班号
            plane_nos = plane_no.split(" + ")
            if any(any(code in pn for code in target_airlines.keys()) for pn in plane_nos):
                filtered_flights.append(flight)
                logging.info(f"保留目标航司中转航班: {plane_no} - {airline}")
            else:
                logging.debug(f"过滤非目标航司中转航班: {plane_no} - {airline}")
        else:
            # 单个航班：直接检查航班号
            if any(code in plane_no for code in target_airlines.keys()):
                filtered_flights.append(flight)
                logging.info(f"保留目标航司航班: {plane_no} - {airline}")
            else:
                logging.debug(f"过滤非目标航司航班: {plane_no} - {airline}")

    logging.info(f"航班过滤完成: 原始航班数 {len(flights)}, 过滤后航班数 {len(filtered_flights)}")
    return filtered_flights


def all_flights(all_flights_div, departure, arrival, departure_date):
    flights = []  # 用于存储所有航班信息的列表
    for flight_div in all_flights_div[1:]:  # 跳过第一个元素，可能是标题或广告
        flight_info = data_processing(flight_div, departure, arrival, departure_date)
        if flight_info:  # 只添加非None的结果
            flights.append(flight_info)

    # 过滤目标航司的航班
    filtered_flights = filter_target_airlines(flights)
    return filtered_flights


def save_to_mysql(flights):
    """
    使用DatabaseManager将航班数据保存到MySQL数据库
    """
    if not flights:
        logging.warning("没有有效的航班数据可保存。")
        return

    try:
        # 获取DatabaseManager实例
        db_manager = DatabaseManager.get_instance()
        # 保存数据
        db_manager.save_flights(flights)
    except Exception as e:
        logging.error(f"保存数据失败: {str(e)}")
        raise


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def save_to_mysql_with_retry(flights):
    return save_to_mysql(flights)


def flights_page(departure_place, arrive_place, departure_date):
    page = WebPage()
    url = f'https://flights.ctrip.com/online/list/oneway-{departure_place}-{arrive_place}?depdate={departure_date}&cabin=y_s_c_f&adult=1&child=0&infant=0'
    page.get(url)

    time.sleep(5)

    # 滚动页面以加载更多航班
    # for _ in range(4):
    #     time.sleep(1)
    #     page.scroll.to_bottom()
    #     logging.info("页面向下滚动以加载更多航班。")
    #
    # time.sleep(2)
    # 修改滚动方式，确保页面已完全加载
    try:
        # 滚动页面以加载更多航班，使用更安全的滚动方法
        for _ in range(4):
            time.sleep(3)
            # 使用JavaScript直接滚动窗口，避免使用可能为null的document.documentElement
            page.run_js('window.scrollTo(0, document.body.scrollHeight);')
            logging.info("页面向下滚动以加载更多航班。")
    except Exception as e:
        logging.error(f"页面滚动出错: {str(e)}")


    soup = BeautifulSoup(page.html, "html.parser")
    return soup

def get_valid_dates():
    """
    获取未来日期列表，用于航班查询。
    返回：
        list: 日期的列表，格式为'YYYY-MM-DD'
    """
    today = datetime.now().date()
    valid_dates = [(today + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(1, 7)]
    logging.info(f"要查询的日期: {valid_dates}")
    return valid_dates


def main():

    departure_dates = get_valid_dates()
    # 从od.csv文件中读取航线对
    od_pairs = []
    airport_city_dict = {}  # 机场-城市映射字典

    # 读取od.csv文件中的航线对
    try:
        with open('od.csv', mode='r', encoding='UTF-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if len(row) >= 2:  
                    departure = row[0]
                    arrival = row[1]
                    od_pairs.append((departure, arrival))
    except Exception as e:
        logging.error(f"读取od.csv文件时出错：{e}")
        return

    for departure_date in departure_dates:
        for departure, arrival in od_pairs:
            logging.info(f"开始搜索航班：出发地 {departure}({airport_city_dict.get(departure, 'N/A')}) -> 目的地 {arrival}({airport_city_dict.get(arrival, 'N/A')})")
            soup = flights_page(departure, arrival, departure_date)
            if not soup:
                logging.error("未能获取航班页面内容。")
                continue

            all_flights_divs = soup.find_all("div", {"class": "flight-box"})
            if not all_flights_divs:
                logging.warning("没有找到航班信息。")
                continue

            flights = all_flights(all_flights_divs, departure, arrival, departure_date)
            try:
                save_to_mysql_with_retry(flights)
            except Exception as e:
                logging.error(f"在重试后仍然失败: {str(e)}")


# 在程序结束时关闭数据库连接
def cleanup():
    try:
        db_manager = DatabaseManager.get_instance()
        if db_manager:
            db_manager.close()
            logging.info("数据库连接已关闭")
    except Exception as e:
        logging.error(f"关闭数据库连接时出错: {str(e)}")

class TaskManager:
    def __init__(self):
        self.current_task = None
        self.task_start_time = None
        self.first_run = True
        self.max_task_duration = 5 * 60  

    def should_run_job(self):
        """
        检查是否应该运行任务
        """
        if self.first_run:
            return True
        
        current_time = datetime.now()
        return current_time.hour == 0 and current_time.minute <= 40

    def is_task_timeout(self):
        """
        检查当前任务是否超时
        """
        if not self.task_start_time:
            return False
        elapsed_time = (datetime.now() - self.task_start_time).total_seconds()
        return elapsed_time > self.max_task_duration

    def run_task(self):
        """
        运行爬虫任务
        """
        self.task_start_time = datetime.now()
        print(f"[{self.task_start_time}] 定时任务执行中...")
        
        try:
            main()
        except Exception as e:
            logging.error(f"任务执行出错: {str(e)}")
        finally:
            self.task_start_time = None
            if self.first_run:
                self.first_run = False

    def run(self):
        """
        运行任务管理器
        """
        print(f"[{datetime.now()}] 任务管理器启动")
        if self.first_run:
            print("首次运行，立即开始爬取...")
        else:
            print("定时任务已设置，将在每天00:00-00:40之间执行")

        while True:
            try:
                current_time = datetime.now()
                
                # 检查是否应该运行任务
                if self.should_run_job():
                    if not self.task_start_time:  # 如果没有正在运行的任务
                        self.run_task()
                    elif self.is_task_timeout():  # 如果任务超时
                        logging.warning("任务超时，终止当前任务")
                        self.task_start_time = None
                        cleanup()  # 清理资源
                        continue
                
                time.sleep(60)  # 每分钟检查一次
                
            except KeyboardInterrupt:
                print("\n程序被用户中断")
                cleanup()
                break
            except Exception as e:
                logging.error(f"任务管理器出错: {str(e)}")
                if self.task_start_time:
                    self.task_start_time = None
                    cleanup()
                time.sleep(60)  # 发生错误时等待1分钟后继续

if __name__ == "__main__":
    task_manager = TaskManager()
    task_manager.run()

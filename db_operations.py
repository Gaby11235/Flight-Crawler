from sqlalchemy import create_engine, Column, String, DateTime, Float, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

Base = declarative_base()

class Flight(Base):
    """航班数据表模型"""
    __tablename__ = 'flights_raw'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    airline = Column(String(50), nullable=True)
    departure_airport = Column(String(50), nullable=True)
    arrival_airport = Column(String(50), nullable=True)
    departure_time = Column(String(20), nullable=True)
    arrival_time = Column(String(20), nullable=True)
    flight_information = Column(String(200), nullable=True)
    price = Column(Float, nullable=True)
    plane_no = Column(String(50), nullable=True)
    search_departure = Column(String(10), nullable=True)
    search_arrival = Column(String(10), nullable=True)
    search_departure_date = Column(String(20), nullable=True)
    crawl_date = Column(DateTime, default=datetime.now)

class DatabaseManager:
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self, db_url=None):
        if not hasattr(self, 'initialized'):
            self.initialized = True
            self.db_url = db_url or "mysql+pymysql://root:123456@localhost:3306/flight_data"
            self.engine = None
            self.Session = None
            self._init_connection()
    
    def _init_connection(self):
        """初始化数据库连接"""
        try:
            # 创建数据库引擎
            self.engine = create_engine(
                self.db_url,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800,
                pool_pre_ping=True
            )
            
            # 创建会话工厂
            self.Session = sessionmaker(bind=self.engine)
            
            # 创建表（如果不存在）
            Base.metadata.create_all(self.engine)
            
            logging.info("数据库连接初始化成功")
        except Exception as e:
            logging.error(f"数据库连接初始化失败: {str(e)}")
            raise
    
    def save_flights(self, flights):
        """保存航班数据到数据库"""
        if not flights:
            logging.warning("没有航班数据可保存")
            return
        
        session = None
        try:
            session = self.Session()
            
            # 批量插入数据
            flight_objects = []
            for flight in flights:
                # 转换价格字段
                try:
                    price = float(flight['price']) if flight['price'] and flight['price'] != 'null' else None
                except (ValueError, TypeError):
                    price = None
                
                # 创建Flight对象
                flight_obj = Flight(
                    airline=flight['airline'],
                    departure_airport=flight['departure_airport'],
                    arrival_airport=flight['arrival_airport'],
                    departure_time=flight['departure_time'],
                    arrival_time=flight['arrival_time'],
                    flight_information=flight['FlightInformation'],
                    price=price,
                    plane_no=flight['plane_no'],
                    search_departure=flight['search_departure'],
                    search_arrival=flight['search_arrival'],
                    search_departure_date=flight['search_departure_date'],
                    crawl_date=datetime.now()
                )
                flight_objects.append(flight_obj)
            
            # 批量插入
            session.bulk_save_objects(flight_objects)
            session.commit()
            
            logging.info(f"成功保存 {len(flights)} 条航班数据")
            
        except Exception as e:
            if session:
                session.rollback()
            logging.error(f"保存航班数据失败: {str(e)}")
            raise
        finally:
            if session:
                session.close()
    
    def close(self):
        """关闭数据库连接"""
        if self.engine:
            self.engine.dispose()
            logging.info("数据库连接已关闭")
    
    @classmethod
    def get_instance(cls):
        """获取DatabaseManager实例"""
        if not cls._instance:
            cls._instance = cls()
        return cls._instance 
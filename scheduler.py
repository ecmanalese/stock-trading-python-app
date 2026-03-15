import schedule
import time
from script import load_stock_stickers_job

from datetime import datetime

def basic_job(): 
    print("Job started at: ", datetime.now())

# Schedule the job to run every minute
schedule.every().minute.do(basic_job)
schedule.every().minute.do(load_stock_stickers_job)

# SChedule the job to run daily at 9:30 AM
# schedule.every().day.at("09:30").do(basic_job)
# schedule.every().day.at("09:30").do(load_stock_stickers_job)

while True:
    schedule.run_pending()
    time.sleep(1)
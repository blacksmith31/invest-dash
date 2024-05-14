from datetime import datetime


def crontest():
    print(f"{datetime.now()}: This is the test script running")

# in config file:
resource = YFiChart()
trigger = ContinuousSubweekly()
strategy = Strategy()
job = TickerJob(resource, trigger, strategy)
job.get()

if __name__ == "__main__":
    crontest()

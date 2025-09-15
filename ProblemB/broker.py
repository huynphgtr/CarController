import logging
import asyncio
from amqtt.broker import Broker


formatter = "[%(asctime)s] :: %(levelname)s :: %(name)s :: %(message)s"
logging.basicConfig(level=logging.INFO, format=formatter)

config = {
    'listeners': {        
        'default': {
            'type': 'tcp',
            'bind': '0.0.0.0:1883',
            'max_connections': 50,
        },
    },
    'sys_interval': 10,
    'auth': {
        'allow-anonymous': True,
    },
    'topic-check': {
        'enabled': False
    }
}

async def main():
    broker = Broker(config)
    try:
        await broker.start()
        print("Broker is running.Listening TCP on 1883")
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        await broker.shutdown()
        print("Broker stopped.")
    except Exception as e:
        print(f"Error: {e}")
        await broker.shutdown()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nClosing program...")
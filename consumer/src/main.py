from manager import ConsumerManager


def main():

    print("Starting ConsumerManager...", flush=True)

    manager = ConsumerManager()

    print("ConsumerManager created.", flush=True)
    
    manager.start()


if __name__ == "__main__":
    main()
import sys
from manager import SparkManager


def main():

    job_name = None

    if "--job" in sys.argv:
        index = sys.argv.index("--job")

        if index + 1 >= len(sys.argv):
            raise ValueError("--job requires a job name")

        job_name = sys.argv[index + 1]

    manager = SparkManager()
    manager.run(job_name)


if __name__ == "__main__":
    main()
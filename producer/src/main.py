from manager import PipelineManager

from pipelines.factory import get_pipelines


def main():

    manager = PipelineManager()

    for pipeline in get_pipelines():
        manager.add_pipeline(pipeline)

    manager.start()


if __name__ == "__main__":
    main()
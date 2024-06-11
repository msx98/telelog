import runpod
from main import main


def handler(job):
    main()


if __name__ == "__main__":
    runpod.serverless.start({"handler": handler})

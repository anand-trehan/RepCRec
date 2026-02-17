## Run Locally

The project requires Python 3.9+ along with the [tabulate](https://pypi.org/project/tabulate/) package. Install the requirements by running:

```bash
pip install -r requirements.txt
```

To run the database in interactive mod and pass file one by one, run:

```
```bash
python3 main.py
```

To run a single test file:


```
```bash
python3 tests/test01.txt
```

To run all test files and see the output

```bash
./run_tests.sh
```

## Run Using Docker

There is an option to also create a docker image and run the database from that image. To do so, first create the docker image:

```bash
docker build . -t reprec
```

If you want to run the database directly in interactive mode and give operations one by one you will need to attach your `stdin` and `stdout` to the container as well as run it in interactive mode. The following command takes care of everything

```bash
docker run -a stdin -a stdout -i -t reprec
```

To simply enter the docker and run different scripts for testing run:

```bash
docker run -it --entrypoint="" reprec bash
```

This will take you inside the docker container and all the commands for running and testing the database from above will be valid.

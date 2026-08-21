"""A Metaflow flow exercising the graph shapes that map onto Flyte 2 differently.

Run it unchanged on either side::

    python showcase_flow.py run                    # plain Metaflow
    pyflyte-metaflow run showcase_flow.py          # Flyte 2, locally
    pyflyte-metaflow run --remote --datastore-root s3://bucket/mf showcase_flow.py

``start`` fans out with ``foreach`` (a Flyte ``@dynamic`` expansion), ``join``
collects the results, and ``report`` summarises — so a single run covers
parameters, fan-out, and a join.
"""

from metaflow import FlowSpec, Parameter, current, step


class ShowcaseFlow(FlowSpec):
    """Square a range of numbers in parallel, then summarise."""

    count = Parameter("count", default=4, help="How many numbers to square.")
    label = Parameter("label", default="showcase", help="Label for the report.")

    @step
    def start(self):
        print(f"Metaflow run id: {current.run_id}")
        self.numbers = list(range(1, self.count + 1))
        self.next(self.square, foreach="numbers")

    @step
    def square(self):
        self.squared = self.input * self.input
        print(f"{self.input}^2 = {self.squared}")
        self.next(self.join)

    @step
    def join(self, inputs):
        self.total = sum(i.squared for i in inputs)
        self.next(self.report)

    @step
    def report(self):
        print(f"[{self.label}] run={current.run_id} total={self.total}")
        self.next(self.end)

    @step
    def end(self):
        print(f"[{self.label}] done: total={self.total}")


if __name__ == "__main__":
    ShowcaseFlow()

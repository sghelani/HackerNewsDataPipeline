import csv
from collections import deque
import itertools


class DAG:
    """
    This class implements a Directed Acyclic Graph
    """
    def __init__(self):
        self.graph = {}

    def in_degrees(self):
        """
        This method calculates the in-degrees of all the nodes in the graph
        """
        in_degrees = {}
        for node in self.graph:
            if node not in in_degrees:
                in_degrees[node] = 0
            for pointed in self.graph[node]:
                if pointed not in in_degrees:
                    in_degrees[pointed] = 0
                in_degrees[pointed] += 1
        return in_degrees

    def sort(self):
        """
        The method will sort the graph nodes based on the indegrees.
        At every step the nodes with indgree = 0 will be added to the queue.
        These nodes will be deleted from the DAG and the process is repeated again,until all the nodes are deleted.
        """
        in_degrees = self.in_degrees()
        to_visit = deque()
        for node in self.graph:
            if in_degrees[node] == 0:
                to_visit.append(node)

        searched = []
        while to_visit:
            node = to_visit.popleft()
            for pointer in self.graph[node]:
                in_degrees[pointer] -= 1
                if in_degrees[pointer] == 0:
                    to_visit.append(pointer)
            searched.append(node)
        return searched

    def add(self, node, to=None):
        """
        This method adds the node and the edge(relationship between two nodes) to the graph
        Parameters
        ----------
        node: 
           Node in the DAG
        to: 
           Node in DAG which the first node is related to 
        """
        if node not in self.graph:
            self.graph[node] = []
        if to:
            if to not in self.graph:
                self.graph[to] = []
            self.graph[node].append(to)
        if len(self.sort()) != len(self.graph):
            raise Exception


class Pipeline:
    def __init__(self):
        """
        Pipeline is implemented as a Directed Acyclic Graph
        """
        self.tasks = DAG()

    def task(self, depends_on=None):
        """
        This method will return a function decorator
        """
        def inner(f):
            """
            This will add the task and the relationship to the pipeline of tasks
            """
            self.tasks.add(f)
            if depends_on:
                self.tasks.add(depends_on, f)
            return f
        return inner

    def run(self):
        """
        This will sort the tasks in the pipeline and run the tasks in the sorted order
        """
        scheduled = self.tasks.sort()
        completed = {}

        for task in scheduled:
            for node, values in self.tasks.graph.items():
                if task in values:
                    completed[task] = task(completed[node])
            if task not in completed:
                completed[task] = task()
        return completed


def build_csv(lines, header=None, file=None):
    """
    Utility function to build CSV file
    
    Parameters
    ----------
    lines: list
        list of lists containing the rows of the file
    header: list
        Header line of the CSV file
    file: str
        Filename 
    """
    if header:
        lines = itertools.chain([header], lines)
    writer = csv.writer(file, delimiter=',')
    writer.writerows(lines)
    file.seek(0)
    return file

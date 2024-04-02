import sys

class Node:
    def __init__(self, parent, key):
        self._key = key
        self._parent = parent
        self._children = []
        self._start = None
        self._cum = 0

    def start(self, time):
        self._start = time

    def end(self, time):
        self._cum += time - self._start

    def get_child(self, key):
        for node in self._children:
            if node._key == key:
                return node
        return None

    def has_child(self, key):
        return self.get_child(key) is not None

    def make_child(self, key):
        new_node = Node(self, key)
        self._children.append(new_node)
        return new_node

    def parent(self):
        return self._parent

    def cumulative_time(self):
        return self._cum

    def __repr__(self, indent=0):
        return (
            "\t"*indent + f"{self._key}: {self._cum}\n" + 
            "".join(c.__repr__(indent=indent+1) for c in self._children)
        )


def parse_timing(line):
    label, time = line.split()
    key, status = label.split(":")
    return key, status, float(time)

def timing(path):
    jobs = []
    node = None

    with open(path, "r") as f:
        for l in f:

            line = l.strip()
            if len(line) == 0:
                continue
            if line.startswith("sdiskio"):
                continue
            if line.startswith("snetio"):
                continue
            if line.startswith("mprof"):
                continue
            if line.startswith("running new process"):
                continue

            key, status, time = parse_timing(line)

            if key == "job" and status == "enter":
                node = Node(None, key)
                node.start(time)

            elif key == "job" and status == "exit":
                node.end(time)
                jobs.append(node)
                node = None

            elif status == "enter" and node.has_child(key):
                node = node.get_child(key)
                node.start(time)

            elif status == "enter" and not node.has_child(key):
                node = node.make_child(key)
                node.start(time)

            elif status == "exit":
                node.end(time)
                node = node.parent()

    return jobs

def parse_io(line):
    _, body = line.split("(")
    body = body[:-1]
    entries = body.split(", ")
    data = {}
    for entry in entries:
        key, value = entry.split("=")
        data[key] = int(value)
    return data

def net_io(path):
    jobs = []
    current = None
    with open(path, "r") as f:
        for l in f:

            line = l.strip()
            if len(line) == 0:
                continue
            if not line.startswith("snetio"):
                continue

            data = parse_io(line)

            if current is None:
                current = data
            else:
                current = {
                    key: data[key] - current[key]
                    for key in current.keys()
                }
                jobs.append(current)
                current = None

    return jobs

if __name__ == "__main__":
    path = sys.argv[1]
    job = timing(path)[0]
    print(f"Runtime: {job.cumulative_time()} s")
    job = net_io(path)[0]
    print(f"Download: {job["bytes_recv"]/2**30} GiB")

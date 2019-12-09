from typing import Any, Dict, List

import json


# parse a list of labels of the form
#   'a.b.c': 'x'
# into a dictionary.
def parse_labels(labels: Dict[str, str]) -> Dict[str, Any]:

    def set_key(d: Dict[str, Any], key: List[LabelPathElement], value: str):
        head = key[0]
        tail = key[1:]

        if len(tail) == 0:
            name = head.name
            if head.is_list_entry:
                if name not in d:
                    d[name] = []
                # a value (i.e. no further path elements) is added to the list
                assert head.index == len(d[name])
                d[name].append(value)
            else:
                d[name] = value
        else:
            nested = head.apply(d)
            set_key(nested, tail, value)

    result = {}
    for key, value in labels.items():
        parts = parse_label(key)
        try:
            value = json.loads(value)
        except ValueError:
            pass
        set_key(result, parts, value)

    return result


class LabelPathElement:

    def __init__(self, name, index=None):
        self.name = name
        self.index = index

    @property
    def is_list_entry(self):
        return self.index is not None

    def apply(self, d):
        if not self.is_list_entry:
            # The head is not a list
            d[self.name] = d.get(self.name, {})
            nested = d[self.name]
        else:
            # The head is indeed a list, add a new entry or
            # update an existing one.
            d[self.name] = d.get(self.name, [])
            if self.index > len(d[self.name]) or self.index < len(d[self.name]) - 1:
                raise KeyError("Indices must be defined in order")

            # traverse into the nested element
            if self.index == len(d[self.name]):
                nested = {}
                d[self.name].append(nested)
            else:
                nested = d[self.name][self.index]

        return nested


def parse_label(label: str) -> List[LabelPathElement]:
    parts = label.split('.')

    elements = []
    while len(parts) > 0:
        head = parts[0]
        parts = parts[1:]

        # determine if the current head is a list by peeking ahead.
        # This is the case if the next path element is numeric.
        # We'll be processing two path elements at once (name/head, index)
        if len(parts) > 0:
            try:
                index = int(parts[0])
                parts = parts[1:]
                elements.append(LabelPathElement(head, index))
                continue
            except ValueError:
                pass

        elements.append(LabelPathElement(head, None))

    return elements

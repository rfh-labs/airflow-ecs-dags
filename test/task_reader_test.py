from ecs.label_parser import parse_labels


def test_parse_labels():
    labels = parse_labels({
        "a.b.c": "1",
        "a.list.0.a": "ha",
        "a.list.0.x": "hb",
        "a.list.1": "1"
    })
    assert labels == {
        'a': {
            'b': {
                'c': 1
            },
            'list': [
                {
                    'a': 'ha',
                    'x': 'hb'
                },
                1
            ]
        }
    }


def test_first_entry_in_list_is_value():
    labels = parse_labels({
        "a.list.0": "1",
        "a.list.1": "2"
    })
    assert labels == {
        'a': {
            'list': [
                1,
                2
            ]
        }
    }

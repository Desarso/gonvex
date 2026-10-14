#!/usr/bin/env python3
"""Summarize CDP snapshot shallow bytes by constructor and replica record shape.

Usage: python heap-report.py .generated/<name>.heapsnapshot [...]
Not a dominator/retained-size calculation; use post-GC CDP heap for that measure.
"""
import collections
import json
import sys
from pathlib import Path

reports = []
for filename in sys.argv[1:]:
    snapshot = json.loads(Path(filename).read_text())
    meta = snapshot['snapshot']['meta']
    fields, edge_fields = meta['node_fields'], meta['edge_fields']
    width, edge_width = len(fields), len(edge_fields)
    nodes, edges, strings = snapshot['nodes'], snapshot['edges'], snapshot['strings']
    types = meta['node_types'][0]
    edge_types = meta['edge_types'][0]
    totals = collections.defaultdict(lambda: {'count': 0, 'shallowBytes': 0})
    shapes = collections.defaultdict(lambda: {'count': 0, 'shallowBytes': 0})
    offset = 0
    for i in range(0, len(nodes), width):
        kind = types[nodes[i + fields.index('type')]]
        name = strings[nodes[i + fields.index('name')]]
        size = nodes[i + fields.index('self_size')]
        count = nodes[i + fields.index('edge_count')]
        constructor = name if kind == 'object' else kind
        totals[constructor]['count'] += 1
        totals[constructor]['shallowBytes'] += size
        if kind == 'object' and name == 'Object':
            properties = set()
            for j in range(offset, offset + count * edge_width, edge_width):
                if edge_types[edges[j + edge_fields.index('type')]] == 'property':
                    properties.add(strings[edges[j + edge_fields.index('name_or_index')]])
            # V8 omits immediate numeric fields from reference edges.
            for label, required in [
                ('treap node', {'value','owner'}),
                ('treap entry', {'id','value'}),
                ('recency record', {'key','entity','id'}),
                ('slot page', {'owner','ids','values'}),
                ('hash bucket', {'owner','slots'}),
            ]:
                if required <= properties:
                    shapes[label]['count'] += 1
                    shapes[label]['shallowBytes'] += size
        offset += count * edge_width
    reports.append({'file': Path(filename).name,
                    'constructors': dict(sorted(totals.items(), key=lambda item: -item[1]['shallowBytes'])[:12]),
                    'replicaShapes': dict(shapes)})
    del snapshot, nodes, edges, strings
print(json.dumps(reports, indent=2))

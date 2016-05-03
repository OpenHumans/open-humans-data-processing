import bz2
import csv
import random
import sys

import matplotlib.colors as colors
import matplotlib.pyplot as plt

PIE_CHART_MAX_FRAC = 0.30
PIE_CHART_MIN_FRAC = 0.004


def get_counts(filepath):
    """
    Aggregate counts for taxonomic categories in Wild Life of Our Homes data.

    Creates nested dict objects. The counts at any given level are stored in
    the key 'counts'. All other keys refer to taxonomic names for the next
    level, each of which (in turn) has values that are dicts in the same format
    """
    counts = {}

    with bz2.BZ2File(filepath) as f:
        count_rows = csv.reader(f)

        # Store counts for every column other than the first and last.
        header = count_rows.next()
        counts = {k: {} for k in header[1:-1]}

        # Track failed sample columns. These have empty values in the csv.
        failed_sample_columns = []

        for row in count_rows:
            if not any([int(x) for x in row[1:-1] if x]):
                continue
            classification = row[-1].split('/')
            for i in range(len(row[1:-1])):
                if i in failed_sample_columns:
                    continue
                count = 0
                try:
                    count = int(row[i + 1])
                except ValueError:
                    del(counts[header[i + 1]])
                    failed_sample_columns.append(i)
                if count == 0:
                    continue
                counts_level = counts[header[i + 1]]
                while classification:
                    category = classification.pop(0)
                    if category in counts_level:
                        counts_level[category]['count'] += count
                    else:
                        counts_level[category] = {'count': count}
                    counts_level = counts_level[category]
    return counts


def get_summary_counts(counts, max_counts, min_counts, path):
    """
    Get summary counts from the total counts.

    This works iteratively to return a list of tuples:
    [(count, taxonomic_path), ...]

    The function calls itself for any subcategories whose counts exceed
    'max_counts'. Otherwise it returns counts and current path. Categories
    with counts less than 'min_counts' are not reported.
    """
    summary_counts = []
    total = sum([counts[cat]['count'] for cat in counts if cat != 'count'])
    categories = sorted(counts.keys())
    unclassified = 0
    if 'count' in counts:
        this_count = counts.pop('count')
        categories = sorted(counts.keys())
        unclassified = this_count - total
        if unclassified > 0:
            counts['Unclassified'] = {'count': unclassified}
            categories.append('Unclassified')
    for category in categories:
        if (counts[category]['count'] > max_counts and
                [k for k in counts[category] if k != 'count']):
            summary_counts += get_summary_counts(
                counts[category],
                max_counts=max_counts,
                min_counts=min_counts,
                path=path+[category])
        else:
            if counts[category]['count'] >= min_counts:
                summary_counts += [
                    (path+[category], counts[category]['count'])]

    return summary_counts


def add_pie_chart(summary_counts, sample_name, fig, graph_i, graphs_num):
    """
    Add a pie chart to a Wild Life of Our Homes data visualization figure.
    """
    ax = fig.add_axes([0.25, 0.02 + (0.98 / graphs_num) * graph_i, 0.50, (0.98 / graphs_num)])
    ax.set_aspect(1)
    color_set = [c for c in colors.cnames if
                 sum(colors.hex2color(colors.cnames[c])) < 2.5 and
                 sum(colors.hex2color(colors.cnames[c])) > 1]
    random.shuffle(color_set)
    color_set = color_set[0:len(summary_counts)]
    pie_chart = ax.pie(
        [sc[1] for sc in summary_counts],
        labels=['/'.join(sc[0]) for sc in summary_counts],
        labeldistance=1.05,
        pctdistance=0.67,
        colors=color_set,
        autopct='%1.1f%%')
    center_circle = plt.Circle((0, 0), 0.75, color='white', fc='white')
    fig = plt.gcf()
    fig.gca().add_artist(center_circle)
    for pie_wedge in pie_chart[0]:
        pie_wedge.set_edgecolor('white')
    for t in pie_chart[1]:
        t.set_size('smaller')
    for t in pie_chart[2]:
        t.set_size('x-small')
    ax.set_title(sample_name)
    ax.text(-0.6, -1.35, 'Groups with less than 0.4% not depicted.')


def make_pie_charts(counts, output_filepath):
    """
    Make pie chart visualizations for Wild Life of Our Homes data.

    Input data is expected to match the output of the get_counts function.
    """
    graphs_num = len(counts)
    fig = plt.figure(figsize=(16, 10 * graphs_num))
    sample_i = 0
    for sample in counts:
        sample_counts = counts[sample]
        total = sum([sample_counts[cat]['count'] for cat in sample_counts])
        summary_counts = get_summary_counts(
            counts=sample_counts,
            max_counts=int(total * PIE_CHART_MAX_FRAC),
            min_counts=int(total * PIE_CHART_MIN_FRAC),
            path=[])
        summary_counts.reverse()
        add_pie_chart(summary_counts=summary_counts,
                      sample_name=sample,
                      fig=fig,
                      graphs_num=len(counts),
                      graph_i=sample_i)
        sample_i += 1
    fig.savefig(output_filepath)


def generate_manually():
    input_filename = sys.argv[1]
    counts = get_counts(filepath=input_filename)
    output_filename = input_filename.split('.')[0] + '-graphs.png'

    make_pie_charts(counts, output_filename)


if __name__ == '__main__':
    generate_manually()

from itertools import cycle
import pandas as pd
import matplotlib.pyplot as plt


def visualize_winrates(*args, **kwargs):
    """
    Read backend stats from tmp and store winrates per hour per backend
    """
    templates_dict = kwargs.pop('templates_dict')
    df = pd.read_csv(templates_dict.pop('stats_file'))
    df['winrate'] = df.wins / df.bids.apply(float)
    _, ax = plt.subplots()
    colors = cycle([
        'blue',
        'green',
        'black',
        'red',
        'yellow',
        'brown',
        'orange',
        'purple',
        'pink'
    ])
    for backend, group in df.groupby(["backend_name"]):
        ax = group.plot(
            ax=ax,
            kind='line',
            x='hour',
            y='winrate',
            c=next(colors),
            label=backend,
            figsize=(10, 5)
        )
        ax.legend(loc='best', prop={'size': 6})
    ax.get_figure().savefig(templates_dict.pop('graph_file'))

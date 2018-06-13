from itertools import cycle
import pandas as pd
import matplotlib.pyplot as plt


def visualize_winrates(*args, **kwargs):
    """
    Read backend stats from tmp and store winrates per hour per backend
    """
    execution_date = kwargs.pop('execution_date').strftime('%Y-%m-%d')
    df = pd.read_csv("/tmp/winrate_stats/{}/stats.csv".format(execution_date))
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
            figsize=(20, 10)
        )
    ax.legend(loc='best')
    ax.get_figure().savefig("/tmp/winrate_stats/{}/viz.png".format(execution_date))

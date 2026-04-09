import numpy as np
import plotly.graph_objects as go

import kaleido

HANG_THRESHOLD = 4_914_834


async def test_large_fig():
    fig = go.Figure(
        data=[
            go.Scatter(
                x=np.arange(start=0, stop=HANG_THRESHOLD, dtype=float),
                y=np.arange(start=0, stop=HANG_THRESHOLD, dtype=float),
            )
        ]
    )
    assert isinstance(await kaleido.calc_fig(fig), bytes)

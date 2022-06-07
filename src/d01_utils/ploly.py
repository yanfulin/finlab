import plotly.express as px
import plotly.graph_objects as go
from IPython.display import HTML
import plotly.express as px
import plotly.io as pio
import io
import numpy as np
from matplotlib import pyplot as plt
import matplotlib.image as mpimg
import plotly

pio.renderers.default = 'png'
def plot(fig):
    img_bytes = fig.to_image(format="png")
    fp = io.BytesIO(img_bytes)
    with fp:
        i = mpimg.imread(fp, format='png')
    plt.axis('off')
    plt.imshow(i, interpolation='nearest')
    plt.show()





# df_data = px.data.iris()
# df_data
# fig = px.histogram(df_data, x="sepal_width")
# HTML(fig.to_html())
# fig.show()
# print("response")

import plotly.graph_objects as go
fig = go.Figure(
    data=[go.Bar(y=[2, 1, 3])],
    layout_title_text="A Figure Displayed with fig.show()"
)
# fig.show(renderer="svg")
# plot(fig)

plotly.offline.init_notebook_mode(connected=True)

iris = px.data.iris()

iris_plot = px.scatter(iris, x='sepal_width', y='sepal_length',
           color='species', marginal_y='histogram',
          marginal_x='box', trendline='ols')

plotly.offline.plot(iris_plot)
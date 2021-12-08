import matplotlib.pyplot as plt

# I couldn't find any library or function in Scala that would have
# plotted the results in a way that matplotlib.pyplto does in Python
# so I decided to plot the grap by Python.

def kElbowPlot():
    xData = [2,3,4,5,6,7,8,9,10]
    yData = [70.90352230279227,36.74508551706644,26.359705974943004,
             20.237427575385396,9.615902775402317,7.430250617040396,
             6.769915249843031,6.087648037705742,5.77699180993815]
    plt.figure(1);
    plt.plot(xData, yData, color='green', linestyle='dashed', linewidth=1, marker='o', markerfacecolor='red'
             ,markersize=10)
    plt.xlabel("Amount of clusters")
    plt.ylabel("Cost")
    plt.title("Cost of clustering as the function of amount of clusters")
    plt.show()

kElbowPlot()
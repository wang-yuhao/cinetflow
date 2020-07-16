import matplotlib.pyplot as plt

 
def plot_time_tracking(y, x, plot_name):
        print("x: ", x)
        print("y: ", y)
        plt.plot(x, y)
        plt.savefig(plot_name)

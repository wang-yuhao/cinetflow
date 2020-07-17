import matplotlib.pyplot as plt

 
def plot_time_tracking(y, x, plot_name):
        plt.plot(x, y)
        plt.savefig(plot_name)

import numpy as np
import matplotlib.pyplot as plt

test_scores = [55,45,88,75,43,56,89,98,55,54,65,77,88,81,82,89,92,98]

time_spent  = [12,10,22,24,34,56,55,45,47,34,55,43,32,21,15,46,21,34]

plt.scatter(time_spent, test_scores)
plt.xlabel('time_spent on test')
plt.ylabel('test_score')
plt.title('test score vs time spent')

plt.show()


x = [1,2,3,4,5]
y1 = [2,3,2,4,2]
y2 = [8,8,6,7,6]

plt.scatter(x, y1 , marker='o', color='c')
plt.scatter(x, y2 , marker='v', color='m')

plt.show()

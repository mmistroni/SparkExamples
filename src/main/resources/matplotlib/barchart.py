import numpy as np
import matplotlib.pyplot as plt

test_scores = [55,45,88,75,43,56,89,98,55,54,65,77,88,81,82,89,92,98]

##x =[x for x in range(len(test_scores))]
##
##plt.bar(x, test_scores)
##
##plt.show()

bins = [10,20,30,40,50,60,70,80,90,100]

plt.hist(test_scores, bins, histtype='bar', cumulative=True, rwidth=0.8)

plt.show()


'''
x = [2,4,6,8,10]
x2 = [1,3,5,7,9]
y = [4,7,4,7,3]
y2 = [5,3,2,6,2]

plt.bar(x, y, label='One', color='m')
plt.bar(x2, y2, label='Two', color='g')

plt.xlabel('bar number')
plt.ylabel('bar height')
plt.title('Bar char tutorial')

plt.legend()

plt.show()

'''

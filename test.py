# import os
# import shutil
# from time import sleep
#
# while True:
#     for file in os.listdir(r'C:\Users\Abdykarim.D\Documents'):
#         if 'doc' in file and '.pdf' in file:
#             print(file)
#             try:
#                 shutil.move(fr'C:\Users\Abdykarim.D\Documents\{file}', fr'C:\Users\Abdykarim.D\Documents\hueta\{file}')
#             except:
#                 sleep(1)
#                 shutil.move(fr'C:\Users\Abdykarim.D\Documents\{file}', fr'C:\Users\Abdykarim.D\Documents\hueta\{file}')


import numpy as np
import cv2
import matplotlib.pyplot as plt
import matplotlib.image as mpimg


def segment_image(image_path):
    image = cv2.imread(image_path)

    image = mpimg.imread(image_path)

    image = cv2.resize(image, (800, 600))

    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    cv2.imshow('Gray Image', gray)

    _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)

    kernel = np.ones((3, 3), np.uint8)
    opening = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel, iterations=2)
    # sure background area
    sure_bg = cv2.dilate(opening, kernel, iterations=3)
    # Finding sure foreground area
    dist_transform = cv2.distanceTransform(opening, cv2.DIST_L2, 5)
    ret, sure_fg = cv2.threshold(dist_transform, 0.01 * dist_transform.max(), 255, 0)
    # cv2.imshow('Sure Image', sure_fg)

    sure_fg = np.uint8(sure_fg)
    unknown = cv2.subtract(sure_bg, sure_fg)

    cv2.imshow('Binary Image', binary)
    cv2.imshow('Kekus Image', unknown)
    ret, markers = cv2.connectedComponents(sure_fg)
    markers = markers + 1
    markers[unknown == 255] = 0

    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (5, 5))
    morph = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel, iterations=2)

    contours, _ = cv2.findContours(morph, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    # cv2.imshow('Contours Image', contours)

    mask = np.zeros_like(image)

    cv2.drawContours(mask, contours, -1, (0, 255, 0), thickness=cv2.FILLED)

    segmented_image = cv2.bitwise_and(image, mask)

    return segmented_image


image_path = 'C:\\Users\\Abdykarim.D\\Downloads\\Обои\\kek.jpg'
segmented_image = segment_image(image_path)

# cv2.imshow('Original Image', cv2.imread(image_path))

# cv2.imshow('Segmented Image', segmented_image)
# cv2.resizeWindow('Segmented Image', (600, 400))

image = mpimg.imread(image_path)
# image1 = mpimg.imread(segmented_image)
# plt.imshow(image)
# plt.imshow(segmented_image)
# plt.show()
cv2.waitKey(0)
cv2.destroyAllWindows()




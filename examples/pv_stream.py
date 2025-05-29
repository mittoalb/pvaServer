#!/usr/bin/env python
import time
import numpy as np
import pvaccess as pva
from functools import wraps
from pvaserver.util import ntnda_stream

"""
    The ImageJ running the NTNDA pluging must run in the same environment of the pvaserver
"""

@ntnda_stream('UI:IMG')
def build_image(param):
    """Return a 512×512 uint8 image whose intensity depends on *param*."""
    y, x = np.ogrid[:512, :512]
    return ((x + y + param) & 0xFF).astype(np.uint8)


import tkinter as tk

root = tk.Tk()
root.title("Slider → NTNDA")

def on_slider(val):
    build_image(int(float(val)))

slider = tk.Scale(root, from_=0, to=255, orient='horizontal',
                  command=on_slider, length=400)
slider.pack(padx=20, pady=20)

tk.Button(root, text="Send random", command=lambda: build_image(
          np.random.randint(0, 256))).pack(pady=10)

root.mainloop()

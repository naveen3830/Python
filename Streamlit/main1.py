import streamlit as st 
import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt 
import matplotlib
import os

# set page configuration
st.set_page_config(page_title="Data Visualization",layout="centered",page_icon='📊')

#set title
st.title("📊 Data Visualization- Web App")

#Getting the working directory of main1.py
working_dir=os.path.dirname(os.path.abspath(__file__))

folder_path=f"{working_dir}/data"
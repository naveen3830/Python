import streamlit as st

st.title("This is a test web page")
st.subheader("This is a sub heading")
st.header("This is a header")
st.text("This function is similar to the paragraph function in the html tag. It is used to display text in the web page")

st.markdown(" **Hello** World")
st.markdown(" *Hello* World")
st.markdown("---")
st.markdown("[Markdown Cheatsheet](https://www.markdownguide.org/cheat-sheet/)")

st.write(" # This is H1 tag")
st.write(" ## This is H2 tag")
st.write(" ### This is H3 tag")

st.write(">Blockquote: This is a blockquote This is the second line It seems like you are trying to create a blockquote using the st.write method with Markdown syntax. In Streamlit, you can use the st.markdown method to render Markdown content, including blockquotes. Here's an example ")

st.text("It seems like you are trying to create a blockquote using the st.write method with Markdown syntax. In Streamlit, you can use the st.markdown method to render Markdown content, including blockquotes. Here's an example:")

st.markdown("It seems like you are trying to create a blockquote using the st.write method with Markdown syntax. In Streamlit, you can use the st.markdown method to render Markdown content, including blockquotes. Here's an example:")

definition_text = "This is used to describe definition"
st.markdown(f"**Definition:** {definition_text}")

st.write("""
            1. First item
            2. Second item
            3. Third item
            """)

st.write("""
        - First item
        - Second item
        - Third item
        """)

code="""
d1.dropna(inplace=True)
d1["lead_actor"]=d1['actors'].str.split("|").apply(lambda x:x[0])
d1
"""
st.code(code,language='python')

st.write("---")
c="""
CREATE TABLE users (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
"""
st.code(c,language='sql')

image="https://img.freepik.com/free-photo/painting-mountain-lake-with-mountain-background_188544-9126.jpg?size=626&ext=jpg&ga=GA1.1.1448711260.1706745600&semt=sph "
st.image(image,caption="Nature image",use_column_width=True)

def main():
    st.title("Embed PowerPoint in Streamlit")

    # Replace the following line with your PowerPoint embed code
    embed_code = """
    <iframe src="https://1drv.ms/p/c/065730af9b68b8c1/IQM_jAgsyDVyT5sxAXciQPT7AQOmThlidJ4Ag9U7P6yEzc4?wdAr=1.7777777777777777" width="100%" height="600px" frameborder="0" scrolling="no" style="border: 0; margin: 0; padding: 0;>This is an embedded <a target="_blank" href="https://office.com">Microsoft Office</a> presentation, powered by <a target="_blank" href="https://office.com/webapps">Office</a>.</iframe>
    """

    st.markdown(embed_code, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
    
def main():
    st.title("Custom Background in Streamlit")

    # Your CSS code for the background
    custom_css = """
    <style>
        body {
            background-color: #e5e5f7;
            opacity: 1;
            background-image: repeating-radial-gradient(circle at 0 0, transparent 0, #e5e5f7 11px),
                              repeating-linear-gradient(#01f4d155, #01f4d1);
        }
    </style>
    """

    # Apply the custom CSS
    st.markdown(custom_css, unsafe_allow_html=True)

    # Your Streamlit content goes here
    st.write("This is your Streamlit app with a custom background.")

if __name__ == "__main__":
    main()
    
    


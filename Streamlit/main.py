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

c="""
CREATE TABLE users (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
"""
st.code(c,language='sql')


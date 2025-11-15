import streamlit as st
from modules import summarizer, quiz_generator, chatbot_rag, utils, vectorstore
import os

st.set_page_config(page_title="🧠 AI Study Assistant", layout="wide")
DOC_PATH = "data/latest_doc.txt"

st.title("📚 AI Study Assistant")

# Upload document
uploaded_file = st.sidebar.file_uploader("Upload Document", type=["pdf", "txt"])

if uploaded_file:
    if uploaded_file.name.endswith(".pdf"):
        text = utils.extract_text_from_pdf(uploaded_file)
    else:
        text = uploaded_file.read().decode("utf-8")

    with open(DOC_PATH, "w", encoding="utf-8") as f:
        f.write(text)

    chunks = utils.chunk_text(text)
    vectorstore.build_vectorstore(chunks)
    st.sidebar.success("✅ Document uploaded and processed!")

# Loading stored document
if os.path.exists(DOC_PATH):
    with open(DOC_PATH, "r", encoding="utf-8") as f:
        doc_content = f.read()

    tab1, tab2, tab3 = st.tabs(["📘 Summarizer", "🧠 Quiz Generator", "💬 Chatbot"])

    with tab1:
        st.subheader("Summary Options")
        mode = st.radio("Type of Summary", ["brief", "detailed"])
        if st.button("Summarize"):
            with st.spinner("Summarizing..."):
                summary = summarizer.summarize_text(doc_content, mode)
                st.markdown("### 📄 Summary")
                st.markdown(summary)

        with tab2:
            st.subheader("🧠 Quiz Generator")

            if "quiz_data" not in st.session_state:
                st.session_state.quiz_data = []

            if "quiz_selected" not in st.session_state:
                st.session_state.quiz_selected = {}

            num_questions = st.slider("Number of questions", 1, 10, 5)

            if st.button("Generate Quiz"):
                with st.spinner("Generating quiz..."):
                    quiz_data = quiz_generator.generate_quiz_questions(doc_content, num_questions)
                    st.session_state.quiz_data = quiz_data
                    st.session_state.quiz_selected = {}  # Reset selection
                    st.success("✅ Quiz ready!")

            if st.session_state.quiz_data:
                if "raw_text" in st.session_state.quiz_data[0]:
                    st.warning("⚠️ Format issue detected. Here's raw:")
                    st.text_area("Model output", st.session_state.quiz_data[0]["raw_text"], height=300)

                else:
                    st.markdown("### ✍️ Answer the Questions:")

                    for idx, q in enumerate(st.session_state.quiz_data):
                        question = q["question"]
                        options = q["options"]

                        selected_option = st.radio(
                            f"Q{idx + 1}: {question}",
                            options,
                            index=None,
                            key=f"q_{idx}"
                        )

                        # Only evaluate once the user selects
                        if selected_option:
                            correct_index = "ABCD".index(q["answer"])
                            correct_option = q["options"][correct_index]

                            # Check if stored already
                            if f"q_{idx}" not in st.session_state.quiz_selected:
                                st.session_state.quiz_selected[f"q_{idx}"] = selected_option

                            if selected_option == correct_option:
                                st.success("✅ Correct!")
                            else:
                                st.error(f"❌ Incorrect. Correct answer is: {q['answer']}. {correct_option}")
                                
        with tab3:
            st.subheader("💬 Document Q&A Chatbot")

            if "chat_history" not in st.session_state:
                st.session_state.chat_history = []

            # Input for user's question
            user_q = st.chat_input("Ask a question related to the uploaded document...")

            if user_q:
                with st.spinner("Thinking..."):
                    answer = chatbot_rag.chatbot_respond(user_q)

                    # Save this exchange to chat history
                    st.session_state.chat_history.append({
                        "role": "user",
                        "content": user_q
                    })
                    st.session_state.chat_history.append({
                        "role": "assistant",
                        "content": answer
                    })

            # Show past chat messages in order
            for msg in st.session_state.chat_history:
                with st.chat_message(msg["role"]):
                    st.markdown(msg["content"])
else:
    st.warning("Please upload a document first (PDF or TXT).")
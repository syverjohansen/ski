import os
from langchain.document_loaders import CSVLoader
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.vectorstores import Chroma
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.llms import Ollama
# Alternative: from langchain_openai import OpenAI
from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate
import pandas as pd
from pathlib import Path

class SkiStatsRAG:
    def __init__(self, base_directory="~/ski/elo/python", model_name="llama3.2"):
        self.base_directory = Path(base_directory).expanduser()
        self.model_name = model_name
        self.vectorstore = None
        self.qa_chain = None
        
        # Define sports and their corresponding directory names
        self.sports = {
            'alpine': 'alpine',
            'biathlon': 'biathlon', 
            'nordic_combined': 'nordic-combined',
            'ski_jumping': 'skijump',
            'cross_country': 'ski'
        }
        
        # Initialize embeddings (free Hugging Face model)
        self.embeddings = HuggingFaceEmbeddings(
            model_name="sentence-transformers/all-MiniLM-L6-v2"
        )
        
        # Initialize local LLM (requires Ollama installed)
        # Option 1: Local Ollama
        self.llm = Ollama(model=model_name)
        
        # Option 2: OpenAI API (uncomment and set OPENAI_API_KEY environment variable)
        # from langchain_openai import OpenAI
        # self.llm = OpenAI(temperature=0)
        
    def load_csv_data(self):
        """Load all CSV files from the sport-specific directories"""
        documents = []
        
        for sport_name, sport_dir in self.sports.items():
            print(f"Loading data for {sport_name}...")
            
            # Define file names based on sport
            if sport_name == 'alpine':
                file_names = ['men_chrono.csv', 'ladies_chrono.csv']
            else:
                file_names = ['men_chrono_elevation.csv', 'ladies_chrono_elevation.csv']
            
            # Check both regular and relay directories
            directories_to_check = [
                self.base_directory / sport_dir / 'polars' / 'excel365',
                self.base_directory / sport_dir / 'polars' / 'relay' / 'excel365'
            ]
            
            for directory in directories_to_check:
                if not directory.exists():
                    print(f"  Directory not found: {directory}")
                    continue
                    
                is_relay = 'relay' in str(directory)
                
                for file_name in file_names:
                    file_path = directory / file_name
                    
                    if file_path.exists():
                        print(f"  Loading {file_path}")
                        
                        # Load CSV with LangChain's CSVLoader
                        try:
                            loader = CSVLoader(
                                file_path=str(file_path),
                                encoding="utf-8",
                                csv_args={'delimiter': ','}
                            )
                            
                            csv_docs = loader.load()
                            
                            # Add rich metadata
                            gender = 'men' if 'men_' in file_name else 'ladies'
                            
                            for doc in csv_docs:
                                doc.metadata.update({
                                    'source_file': file_name,
                                    'sport': sport_name,
                                    'gender': gender,
                                    'is_relay': is_relay,
                                    'full_path': str(file_path),
                                    'data_type': 'elo_chronological'
                                })
                                
                            documents.extend(csv_docs)
                            print(f"    Loaded {len(csv_docs)} records")
                            
                        except Exception as e:
                            print(f"    Error loading {file_path}: {e}")
                    else:
                        print(f"  File not found: {file_path}")
        
        print(f"\nTotal documents loaded: {len(documents)}")
        return documents
    
    def infer_sport_from_filename(self, filename):
        """Legacy method - kept for compatibility but not used in new structure"""
        return "general"
    
    def create_vectorstore(self, documents):
        """Create vector store from documents"""
        print("Creating embeddings and vector store...")
        
        # Split documents if they're too large
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=200,
            separators=["\n\n", "\n", ",", " "]
        )
        
        split_docs = text_splitter.split_documents(documents)
        
        # Create vector store
        self.vectorstore = Chroma.from_documents(
            documents=split_docs,
            embedding=self.embeddings,
            persist_directory="./chroma_db"
        )
        
        print(f"Created vector store with {len(split_docs)} document chunks")
        
    def setup_qa_chain(self):
        """Set up the question-answering chain"""
        
        # Custom prompt template for ski statistics
        prompt_template = """
        You are an expert ski statistics analyst specializing in Elo ratings and chronological performance data. Use the following context from ski data to answer questions about alpine skiing, biathlon, nordic combined, ski jumping, and cross-country skiing.

        The data includes chronological Elo ratings for both men and women across different sports, including both individual and relay events.

        Context from ski statistics database:
        {context}

        Question: {question}

        Provide a detailed answer based on the Elo data. When discussing rankings or statistics, be specific about:
        - Elo rating values and changes over time
        - Gender-specific performance (men vs ladies)
        - Sport-specific comparisons
        - Individual vs relay performance where applicable
        - Trends and patterns in the chronological data

        If the data doesn't contain enough information to answer fully, say so and suggest what additional data might be helpful.

        Answer:
        """
        
        PROMPT = PromptTemplate(
            template=prompt_template,
            input_variables=["context", "question"]
        )
        
        # Create retrieval QA chain
        self.qa_chain = RetrievalQA.from_chain_type(
            llm=self.llm,
            chain_type="stuff",
            retriever=self.vectorstore.as_retriever(
                search_kwargs={"k": 6}  # Retrieve top 6 most relevant chunks
            ),
            chain_type_kwargs={"prompt": PROMPT},
            return_source_documents=True
        )
    
    def initialize(self):
        """Initialize the complete RAG system"""
        print("Initializing Ski Statistics RAG System...")
        
        # Load CSV data
        documents = self.load_csv_data()
        
        # Create vector store
        self.create_vectorstore(documents)
        
        # Setup QA chain
        self.setup_qa_chain()
        
        print("RAG system ready!")
    
    def query(self, question):
        """Query the RAG system"""
        if not self.qa_chain:
            raise ValueError("RAG system not initialized. Call initialize() first.")
        
        print(f"\nQuery: {question}")
        print("-" * 50)
        
        result = self.qa_chain({"query": question})
        
        print("Answer:")
        print(result["result"])
        
        print("\nSources:")
        for i, doc in enumerate(result["source_documents"]):
            sport = doc.metadata.get('sport', 'Unknown')
            gender = doc.metadata.get('gender', 'Unknown')
            is_relay = " (Relay)" if doc.metadata.get('is_relay') else ""
            print(f"{i+1}. {sport.replace('_', ' ').title()} - {gender.title()}{is_relay}")
        
        return result

# Example usage
def main():
    # Initialize the RAG system with the correct base directory
    rag = SkiStatsRAG(base_directory="../elo/python")
    
    # Load and process data
    rag.initialize()
    
    # Example queries
    sample_queries = [
        "What are the current Elo ratings for the top 5 men's alpine skiers?",
        "How have the ladies' biathlon Elo ratings changed over the last season?",
        "Compare men's vs women's performance in nordic combined",
        "Which ski jumping athletes show the biggest Elo rating improvements?",
        "What are the differences between individual and relay Elo ratings in cross-country?",
        "Show me the Elo rating trends for biathlon over time",
        "Who are the highest-rated ladies in each sport?"
    ]
    
    print("\n" + "="*60)
    print("SKI STATISTICS RAG SYSTEM - READY FOR QUERIES")
    print("="*60)
    print("Data loaded from: ~/ski/elo/python/")
    print("Vector database: ~/ski/rag/chroma_db/")
    
    # Interactive query loop
    while True:
        try:
            user_query = input("\nEnter your ski statistics question (or 'quit' to exit): ")
            
            if user_query.lower() in ['quit', 'exit', 'q']:
                break
                
            if user_query.strip():
                rag.query(user_query)
                
        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()
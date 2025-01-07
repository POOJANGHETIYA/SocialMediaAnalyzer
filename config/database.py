from astrapy.db import AsyncAstraDB
import os
from dotenv import load_dotenv
import json
load_dotenv()

class DatabaseConnection:
    def __init__(self):
        self.token = os.getenv("ASTRA_DB_APPLICATION_TOKEN")
        self.api_endpoint = os.getenv("ASTRA_DB_API_ENDPOINT")
        self.collection_name = "social_media_posts"
        self.db = None

    async def connect(self):
        """Connect to AstraDB"""
        try:
            self.db = AsyncAstraDB(
                token=self.token,
                api_endpoint=self.api_endpoint
            )
            print("Successfully connected to AstraDB!")
            return self.db
        except Exception as e:
            print(f"Error connecting to AstraDB: {e}")
            raise

    async def create_collection(self):
        """Create collection if it doesn't exist"""
        try:
            collections = await self.db.get_collections()
            if self.collection_name not in collections:
                await self.db.create_collection(self.collection_name)
                print(f"Collection {self.collection_name} created successfully!")

        except Exception as e:
            print(f"Error creating collection: {e}")
            raise

    async def insert_post(self, post_data):
        """Insert a new post"""
        try:
            collection = await self.db.collection(self.collection_name)
            result = await collection.insert_one(post_data)
            print(f"Successfully inserted post with ID: {result['status']['insertedIds']}")
            return result
        except Exception as e:
            print(f"Error inserting post: {e}")
            raise

    async def get_engagement_metrics(self, post_type=None):
        """Get engagement metrics for posts"""
        try:
            collection = await self.db.collection(self.collection_name)
            
            # Get all posts or filter by post_type
            if post_type:
                response = await collection.find({"post_type": post_type})
            else:
                response = await collection.find({})
            
            # Convert the response to a list of documents
            docs = []
            if isinstance(response, dict) and 'data' in response:
                docs = response['data'].get('documents', [])
            elif isinstance(response, list):
                docs = response
            
            # Process metrics
            metrics = {}
            for doc in docs:
                # Convert string to dict if necessary
                if isinstance(doc, str):
                    doc = json.loads(doc)
                
                post_type = doc.get('post_type')
                if not post_type:
                    continue
                    
                if post_type not in metrics:
                    metrics[post_type] = {
                        'total_posts': 0,
                        'total_likes': 0,
                        'total_shares': 0,
                        'total_comments': 0
                    }
                
                metrics[post_type]['total_posts'] += 1
                metrics[post_type]['total_likes'] += int(doc.get('likes', 0))
                metrics[post_type]['total_shares'] += int(doc.get('shares', 0))
                metrics[post_type]['total_comments'] += int(doc.get('comments', 0))
            
            # Calculate averages and format result
            result = []
            for p_type, data in metrics.items():
                total_posts = data['total_posts']
                if total_posts > 0:
                    result.append({
                        '_id': p_type,
                        'avg_likes': data['total_likes'] / total_posts,
                        'avg_shares': data['total_shares'] / total_posts,
                        'avg_comments': data['total_comments'] / total_posts,
                        'total_posts': total_posts
                    })
            
            return result
        except Exception as e:
            print(f"Error getting metrics: {e}")
            raise
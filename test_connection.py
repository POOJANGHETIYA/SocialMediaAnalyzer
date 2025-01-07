import asyncio
from config.database import DatabaseConnection
import uuid
from datetime import datetime

async def test_connection():
    print("Testing database connection...")
    db_conn = DatabaseConnection()
    
    try:
        # Connect to database
        db = await db_conn.connect()
        print("Successfully connected to database")
        
        # Create collection
        await db_conn.create_collection()
        
        # Test insert
        test_post = {
            "_id": str(uuid.uuid4()),
            "post_type": "reel",
            "content": "Test post content",
            "created_at": datetime.now().isoformat(),
            "likes": 10,
            "shares": 5,
            "comments": 3
        }
        
        # Await the insert operation
        result = await db_conn.insert_post(test_post)
        
        # Wait a short time to ensure the document is indexed
        await asyncio.sleep(1)
        
        # Test get metrics
        metrics = await db_conn.get_engagement_metrics()
        print("\nEngagement Metrics:")
        if metrics:
            for metric in metrics:
                print(f"\nPost Type: {metric['_id']}")
                print(f"Average Likes: {metric['avg_likes']:.2f}")
                print(f"Average Shares: {metric['avg_shares']:.2f}")
                print(f"Average Comments: {metric['avg_comments']:.2f}")
                print(f"Total Posts: {metric['total_posts']}")
        else:
            print("No metrics available yet")
        
        print("\nAll tests completed successfully!")
        
    except Exception as e:
        print(f"Error during testing: {e}")
        raise
    
if __name__ == "__main__":
    asyncio.run(test_connection())
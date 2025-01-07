import asyncio
import random
from datetime import datetime, timedelta
import uuid
from config.database import DatabaseConnection

class MockDataGenerator:
    def __init__(self):
        self.post_types = ['carousel', 'reel', 'static']
        self.content_templates = {
            'carousel': [
                "Swipe through our latest {topic} collection!",
                "10 tips for {topic} - Check all slides",
                "Before and After: {topic} transformation",
            ],
            'reel': [
                "Quick tutorial: How to {topic}",
                "Watch this {topic} hack!",
                "Trending {topic} challenge",
            ],
            'static': [
                "Today's highlight: {topic}",
                "New release: {topic}",
                "Featured {topic} of the day",
            ]
        }
        self.topics = [
            "product launch", "tech tips", "behind the scenes",
            "customer story", "industry news", "team spotlight"
        ]

    def generate_content(self, post_type: str) -> str:
        template = random.choice(self.content_templates[post_type])
        topic = random.choice(self.topics)
        return template.format(topic=topic)

    def generate_engagement_metrics(self, post_type: str) -> dict:
        base_metrics = {
            'carousel': {'likes': (50, 200), 'shares': (10, 50), 'comments': (5, 30)},
            'reel': {'likes': (100, 500), 'shares': (20, 100), 'comments': (10, 50)},
            'static': {'likes': (20, 100), 'shares': (5, 20), 'comments': (2, 15)}
        }
        
        metrics = base_metrics[post_type]
        return {
            'likes': random.randint(*metrics['likes']),
            'shares': random.randint(*metrics['shares']),
            'comments': random.randint(*metrics['comments'])
        }

    def generate_post(self) -> dict:
        post_type = random.choice(self.post_types)
        created_at = datetime.now() - timedelta(days=random.randint(0, 30))
        
        return {
            "_id": str(uuid.uuid4()),
            "post_type": post_type,
            "content": self.generate_content(post_type),
            "created_at": created_at.isoformat(),
            "likes": 0,
            "shares": 0,
            "comments": 0
        }

async def generate_mock_data(num_posts: int = 50):
    print(f"Generating {num_posts} mock posts...")
    
    # Initialize database connection
    db_conn = DatabaseConnection()
    await db_conn.connect()
    await db_conn.create_collection()
    
    # Initialize generator
    generator = MockDataGenerator()
    
    # Generate and insert posts
    for i in range(num_posts):
        # Create post
        post = generator.generate_post()
        
        # Insert post
        await db_conn.insert_post(post)
        
        # Generate and update engagement
        engagement = generator.generate_engagement_metrics(post["post_type"])
        await db_conn.update_post_engagement(post["_id"], engagement)
        
        print(f"Created post {i+1}/{num_posts}: {post['post_type']} with {engagement['likes']} likes")
    
    # Get and display metrics
    print("\nFinal Engagement Metrics:")
    metrics = await db_conn.get_engagement_metrics()
    for metric in metrics:
        print(f"\nPost Type: {metric['_id']}")
        print(f"Average Likes: {metric['avg_likes']:.2f}")
        print(f"Average Shares: {metric['avg_shares']:.2f}")
        print(f"Average Comments: {metric['avg_comments']:.2f}")
        print(f"Total Posts: {metric['total_posts']}")

if __name__ == "__main__":
    asyncio.run(generate_mock_data(50))
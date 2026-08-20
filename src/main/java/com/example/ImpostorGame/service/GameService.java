package com.example.ImpostorGame.service;

import com.example.ImpostorGame.model.GameRoom;
import com.example.ImpostorGame.model.Player;
import com.example.ImpostorGame.model.WordPair;
import com.example.ImpostorGame.phase.GamePhase;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

@Service
public class GameService {

    private final Map<String, GameRoom> rooms =
            new ConcurrentHashMap<>();

    // sessionId -> Player
    private final Map<String, Player> players =
            new ConcurrentHashMap<>();

    // sessionId -> WebSocketSession
    private final Map<String, WebSocketSession> sessions =
            new ConcurrentHashMap<>();

    // One scheduler for all rooms
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(2);


    // ---------------------------------------------------------
    // WORD PAIRS
    // ---------------------------------------------------------

    private final List<WordPair> wordPairs = List.of(
            new WordPair("Beach", "River"),
            new WordPair("Doctor", "Hospital"),
            new WordPair("Coffee", "Tea"),
            new WordPair("Apple", "Orange"),
            new WordPair("Phone", "Camera"),
            new WordPair("Book", "Notebook"),
            new WordPair("Pen", "Pencil"),
            new WordPair("School", "College"),
            new WordPair("Beach", "Island"),
            new WordPair("Forest", "Garden"),
            new WordPair("Cloud", "Smoke"),
            new WordPair("Rain", "Snow"),
            new WordPair("Thunder", "Firework"),
            new WordPair("Sun", "Moon"),
            new WordPair("Star", "Planet"),
            new WordPair("Car", "Taxi"),
            new WordPair("Bicycle", "Scooter"),
            new WordPair("Airport", "Station"),
            new WordPair("Hotel", "Apartment"),
            new WordPair("Kitchen", "Restaurant"),
            new WordPair("Pizza", "Burger"),
            new WordPair("Cake", "Cookie"),
            new WordPair("Chocolate", "Candy"),
            new WordPair("Ice Cream", "Milkshake"),
            new WordPair("Rice", "Pasta"),
            new WordPair("Salt", "Sugar"),
            new WordPair("Honey", "Jam"),
            new WordPair("Butter", "Cheese"),
            new WordPair("Egg", "Omelette"),
            new WordPair("Chicken", "Fish"),
            new WordPair("Tiger", "Lion"),
            new WordPair("Dog", "Wolf"),
            new WordPair("Cat", "Rabbit"),
            new WordPair("Horse", "Zebra"),
            new WordPair("Elephant", "Giraffe"),
            new WordPair("Monkey", "Gorilla"),
            new WordPair("Snake", "Lizard"),
            new WordPair("Shark", "Whale"),
            new WordPair("Dolphin", "Seal"),
            new WordPair("Eagle", "Hawk"),
            new WordPair("Parrot", "Peacock"),
            new WordPair("Butterfly", "Bee"),
            new WordPair("Ant", "Spider"),
            new WordPair("Basketball", "Volleyball"),
            new WordPair("Boxing", "Wrestling"),
            new WordPair("Swimming", "Diving"),
            new WordPair("Running", "Jogging"),
            new WordPair("Chess", "Checkers"),
            new WordPair("Cards", "Dice"),
            new WordPair("Guitar", "Piano"),
            new WordPair("Drum", "Violin"),
            new WordPair("Flute", "Trumpet"),
            new WordPair("Singer", "Actor"),
            new WordPair("Movie", "Series"),
            new WordPair("Comedy", "Drama"),
            new WordPair("Horror", "Thriller"),
            new WordPair("Hero", "Villain"),
            new WordPair("Castle", "Palace"),
            new WordPair("King", "Queen"),
            new WordPair("Prince", "Knight"),
            new WordPair("Pirate", "Sailor"),
            new WordPair("Detective", "Police"),
            new WordPair("Firefighter", "Soldier"),
            new WordPair("Lawyer", "Judge"),
            new WordPair("Chef", "Waiter"),
            new WordPair("Farmer", "Gardener"),
            new WordPair("Pilot", "Astronaut"),
            new WordPair("Engineer", "Architect"),
            new WordPair("Artist", "Painter"),
            new WordPair("Camera", "Telescope"),
            new WordPair("Clock", "Watch"),
            new WordPair("Calendar", "Diary"),
            new WordPair("Map", "Compass"),
            new WordPair("Key", "Lock"),
            new WordPair("Door", "Gate"),
            new WordPair("Chair", "Sofa"),
            new WordPair("Table", "Desk"),
            new WordPair("Bed", "Mattress"),
            new WordPair("Pillow", "Blanket"),
            new WordPair("Lamp", "Candle"),
            new WordPair("Fan", "Air Conditioner"),
            new WordPair("Fridge", "Freezer"),
            new WordPair("Oven", "Microwave"),
            new WordPair("Shower", "Bathtub"),
            new WordPair("Mirror", "Glass"),
            new WordPair("Curtain", "Carpet"),
            new WordPair("Pencil", "Marker"),
            new WordPair("Eraser", "Sharpener"),
            new WordPair("Ruler", "Scale"),
            new WordPair("Bag", "Suitcase"),
            new WordPair("Wallet", "Purse"),
            new WordPair("Shoes", "Sandals"),
            new WordPair("Shirt", "Jacket"),
            new WordPair("Jeans", "Trousers"),
            new WordPair("Hat", "Cap"),
            new WordPair("Glasses", "Sunglasses"),
            new WordPair("Ring", "Bracelet"),
            new WordPair("Necklace", "Watch"),
            new WordPair("Umbrella", "Raincoat"),
            new WordPair("Soap", "Shampoo"),
            new WordPair("Towel", "Napkin"),
            new WordPair("Toothbrush", "Comb"),
            new WordPair("Perfume", "Deodorant"),
            new WordPair("Medicine", "Vitamin"),
            new WordPair("Hospital", "Clinic"),
            new WordPair("Nurse", "Doctor"),
            new WordPair("Heart", "Brain"),
            new WordPair("Eye", "Ear"),
            new WordPair("Hand", "Foot"),
            new WordPair("Finger", "Toe"),
            new WordPair("Hair", "Beard"),
            new WordPair("Blood", "Sweat"),
            new WordPair("Smile", "Laugh"),
            new WordPair("Birthday", "Wedding"),
            new WordPair("Party", "Festival"),
            new WordPair("Gift", "Present"),
            new WordPair("Balloon", "Kite"),
            new WordPair("Fireworks", "Confetti"),
            new WordPair("Museum", "Gallery"),
            new WordPair("Library", "Bookstore"),
            new WordPair("Park", "Playground"),
            new WordPair("Zoo", "Aquarium"),
            new WordPair("Temple", "Church"),
            new WordPair("Market", "Mall"),
            new WordPair("Bank", "ATM"),
            new WordPair("Office", "Factory"),
            new WordPair("Bridge", "Tunnel"),
            new WordPair("Road", "Highway"),
            new WordPair("Street", "Lane"),
            new WordPair("Village", "Town"),
            new WordPair("Country", "State"),
            new WordPair("Desert", "Savanna"),
            new WordPair("Volcano", "Mountain"),
            new WordPair("Cave", "Tunnel"),
            new WordPair("Waterfall", "Fountain"),
            new WordPair("River", "Stream"),
            new WordPair("Lake", "Pond"),
            new WordPair("Tree", "Plant"),
            new WordPair("Flower", "Rose"),
            new WordPair("Grass", "Moss"),
            new WordPair("Leaf", "Branch"),
            new WordPair("Root", "Seed"),
            new WordPair("Garden", "Park"),
            new WordPair("Cactus", "Palm"),
            new WordPair("Coconut", "Mango"),
            new WordPair("Banana", "Papaya"),
            new WordPair("Grape", "Cherry"),
            new WordPair("Lemon", "Lime"),
            new WordPair("Watermelon", "Pineapple"),
            new WordPair("Carrot", "Potato"),
            new WordPair("Tomato", "Onion"),
            new WordPair("Corn", "Peas"),
            new WordPair("Garlic", "Ginger"),
            new WordPair("Taco", "Burrito"),
            new WordPair("Sushi", "Ramen"),
            new WordPair("Soup", "Salad"),
            new WordPair("Popcorn", "Chips"),
            new WordPair("Biscuit", "Cracker"),
            new WordPair("Cereal", "Oatmeal"),
            new WordPair("Milk", "Yogurt"),
            new WordPair("Water", "Juice"),
            new WordPair("Lemonade", "Soda"),
            new WordPair("Coffee", "Cappuccino"),
            new WordPair("Tea", "Green Tea"),
            new WordPair("Cup", "Bottle"),
            new WordPair("Plate", "Bowl"),
            new WordPair("Spoon", "Fork"),
            new WordPair("Knife", "Scissors"),
            new WordPair("Pan", "Pot"),
            new WordPair("Fridge", "Cupboard"),
            new WordPair("Kitchen", "Bedroom"),
            new WordPair("House", "Apartment"),
            new WordPair("Room", "Hall"),
            new WordPair("Balcony", "Terrace"),
            new WordPair("Roof", "Ceiling"),
            new WordPair("Wall", "Fence"),
            new WordPair("Window", "Door"),
            new WordPair("Stairs", "Elevator"),
            new WordPair("Garage", "Parking"),
            new WordPair("Garden", "Balcony"),
            new WordPair("Computer", "Laptop"),
            new WordPair("Keyboard", "Piano"),
            new WordPair("Mouse", "Trackpad"),
            new WordPair("Screen", "Projector"),
            new WordPair("Printer", "Scanner"),
            new WordPair("Speaker", "Headphones"),
            new WordPair("Microphone", "Camera"),
            new WordPair("Charger", "Battery"),
            new WordPair("Cable", "Wire"),
            new WordPair("Internet", "WiFi"),
            new WordPair("Website", "App"),
            new WordPair("Robot", "Drone"),
            new WordPair("Rocket", "Satellite"),
            new WordPair("Planet", "Galaxy"),
            new WordPair("Earth", "Mars"),
            new WordPair("Space", "Sky"),
            new WordPair("Moon", "Cloud"),
            new WordPair("Sunset", "Sunrise"),
            new WordPair("Morning", "Evening"),
            new WordPair("Day", "Week"),
            new WordPair("Summer", "Winter"),
            new WordPair("Spring", "Autumn"),
            new WordPair("Hot", "Warm"),
            new WordPair("Cold", "Cool"),
            new WordPair("Wind", "Breeze"),
            new WordPair("Storm", "Hurricane"),
            new WordPair("Lightning", "Thunder"),
            new WordPair("Fog", "Mist"),
            new WordPair("Rainbow", "Sunset"),
            new WordPair("Fire", "Smoke"),
            new WordPair("Ice", "Snow"),
            new WordPair("Sand", "Dust"),
            new WordPair("Stone", "Rock"),
            new WordPair("Gold", "Silver"),
            new WordPair("Diamond", "Crystal"),
            new WordPair("Iron", "Steel"),
            new WordPair("Wood", "Bamboo"),
            new WordPair("Plastic", "Rubber"),
            new WordPair("Paper", "Cardboard"),
            new WordPair("Glass", "Mirror"),
            new WordPair("Bottle", "Jar"),
            new WordPair("Box", "Basket"),
            new WordPair("Rope", "Chain"),
            new WordPair("Hammer", "Screwdriver"),
            new WordPair("Nail", "Screw"),
            new WordPair("Saw", "Drill"),
            new WordPair("Tool", "Machine"),
            new WordPair("Factory", "Warehouse"),
            new WordPair("Truck", "Van"),
            new WordPair("Bus", "Metro"),
            new WordPair("Train", "Tram"),
            new WordPair("Taxi", "Auto"),
            new WordPair("Car", "Jeep"),
            new WordPair("Motorcycle", "Scooter"),
            new WordPair("Bicycle", "Skateboard"),
            new WordPair("Helmet", "Cap"),
            new WordPair("Wheel", "Tyre"),
            new WordPair("Engine", "Motor"),
            new WordPair("Fuel", "Battery"),
            new WordPair("Airport", "Harbor"),
            new WordPair("Plane", "Helicopter"),
            new WordPair("Ship", "Ferry"),
            new WordPair("Boat", "Yacht"),
            new WordPair("Ticket", "Passport"),
            new WordPair("Suitcase", "Backpack"),
            new WordPair("Hotel", "Hostel"),
            new WordPair("Tourist", "Traveler"),
            new WordPair("Trip", "Journey"),
            new WordPair("Beach", "Resort"),
            new WordPair("Vacation", "Holiday"),
            new WordPair("Camera", "Photo"),
            new WordPair("Picture", "Painting"),
            new WordPair("Artist", "Sculptor"),
            new WordPair("Music", "Song"),
            new WordPair("Singer", "Musician"),
            new WordPair("Guitar", "Ukulele"),
            new WordPair("Piano", "Keyboard"),
            new WordPair("Drum", "Tambourine"),
            new WordPair("Violin", "Cello"),
            new WordPair("Movie", "Film"),
            new WordPair("Actor", "Director"),
            new WordPair("Director", "Producer"),
            new WordPair("Scene", "Stage"),
            new WordPair("Screen", "Canvas"),
            new WordPair("Book", "Novel"),
            new WordPair("Story", "Poem"),
            new WordPair("Author", "Writer"),
            new WordPair("Chapter", "Page"),
            new WordPair("Library", "Museum"),
            new WordPair("Exam", "Quiz"),
            new WordPair("Homework", "Assignment"),
            new WordPair("Student", "Teacher"),
            new WordPair("Classroom", "Laboratory"),
            new WordPair("College", "University"),
            new WordPair("Degree", "Certificate"),
            new WordPair("Science", "Math"),
            new WordPair("Physics", "Chemistry"),
            new WordPair("Biology", "Medicine"),
            new WordPair("History", "Geography"),
            new WordPair("Computer", "Calculator"),
            new WordPair("Code", "Program"),
            new WordPair("Developer", "Designer"),
            new WordPair("Server", "Database"),
            new WordPair("Password", "Username"),
            new WordPair("Email", "Message"),
            new WordPair("Chat", "Call"),
            new WordPair("Video", "Photo"),
            new WordPair("Game", "Puzzle"),
            new WordPair("Player", "Gamer"),
            new WordPair("Controller", "Joystick"),
            new WordPair("Console", "Computer"),
            new WordPair("Football", "Stadium"),
            new WordPair("Cricket", "Bat"),
            new WordPair("Tennis", "Racket"),
            new WordPair("Golf", "Club"),
            new WordPair("Hockey", "Stick"),
            new WordPair("Baseball", "Bat"),
            new WordPair("Boxing", "Gloves"),
            new WordPair("Swimming", "Pool"),
            new WordPair("Running", "Track"),
            new WordPair("Cycling", "Helmet"),
            new WordPair("Yoga", "Meditation"),
            new WordPair("Gym", "Exercise"),
            new WordPair("Coach", "Referee"),
            new WordPair("Team", "Squad"),
            new WordPair("Player", "Captain"),
            new WordPair("Winner", "Champion"),
            new WordPair("Medal", "Trophy"),
            new WordPair("Race", "Competition"),
            new WordPair("Goal", "Score"),
            new WordPair("Ball", "Football"),
            new WordPair("Bat", "Racket"),
            new WordPair("Crowd", "Audience"),
            new WordPair("Stadium", "Arena"),
            new WordPair("King", "Emperor"),
            new WordPair("Queen", "Princess"),
            new WordPair("Castle", "Fortress"),
            new WordPair("Sword", "Shield"),
            new WordPair("Magic", "Spell"),
            new WordPair("Wizard", "Witch"),
            new WordPair("Dragon", "Dinosaur"),
            new WordPair("Ghost", "Monster"),
            new WordPair("Alien", "Robot"),
            new WordPair("Superhero", "Detective"),
            new WordPair("Mask", "Costume"),
            new WordPair("Crown", "Throne"),
            new WordPair("Treasure", "Gold"),
            new WordPair("Pirate", "Captain"),
            new WordPair("Sailor", "Fisherman"),
            new WordPair("Jungle", "Forest"),
            new WordPair("Safari", "Zoo"),
            new WordPair("Lion", "Tiger"),
            new WordPair("Bear", "Wolf"),
            new WordPair("Deer", "Goat"),
            new WordPair("Cow", "Buffalo"),
            new WordPair("Sheep", "Lamb"),
            new WordPair("Pig", "Boar"),
            new WordPair("Chicken", "Duck"),
            new WordPair("Eagle", "Owl"),
            new WordPair("Crow", "Pigeon"),
            new WordPair("Sparrow", "Parrot"),
            new WordPair("Bee", "Butterfly"),
            new WordPair("Ant", "Beetle"),
            new WordPair("Spider", "Scorpion"),
            new WordPair("Frog", "Toad"),
            new WordPair("Turtle", "Tortoise"),
            new WordPair("Whale", "Dolphin"),
            new WordPair("Shark", "Ray"),
            new WordPair("Octopus", "Squid"),
            new WordPair("Crab", "Lobster"),
            new WordPair("Shell", "Pearl"),
            new WordPair("Island", "Peninsula"),
            new WordPair("Ocean", "Sea"),
            new WordPair("Wave", "Tide"),
            new WordPair("Beach", "Coast"),
            new WordPair("Harbor", "Port"),
            new WordPair("Lighthouse", "Tower"),
            new WordPair("Mountain", "Volcano"),
            new WordPair("Valley", "Canyon"),
            new WordPair("River", "Waterfall"),
            new WordPair("Forest", "Jungle"),
            new WordPair("Tree", "Bush"),
            new WordPair("Flower", "Plant"),
            new WordPair("Rose", "Tulip"),
            new WordPair("Sunflower", "Daisy"),
            new WordPair("Garden", "Farm"),
            new WordPair("Farmer", "Cowboy"),
            new WordPair("Cowboy", "Sheriff"),
            new WordPair("Police", "Detective"),
            new WordPair("Doctor", "Surgeon"),
            new WordPair("Nurse", "Pharmacist"),
            new WordPair("Chef", "Baker"),
            new WordPair("Baker", "Butcher"),
            new WordPair("Waiter", "Cashier"),
            new WordPair("Driver", "Pilot"),
            new WordPair("Pilot", "Captain"),
            new WordPair("Teacher", "Professor"),
            new WordPair("Student", "Pupil"),
            new WordPair("Manager", "Boss"),
            new WordPair("Worker", "Employee"),
            new WordPair("Office", "Company"),
            new WordPair("Meeting", "Conference"),
            new WordPair("Project", "Assignment"),
            new WordPair("Team", "Group"),
            new WordPair("Friend", "Neighbor"),
            new WordPair("Family", "Friends"),
            new WordPair("Brother", "Cousin"),
            new WordPair("Father", "Uncle"),
            new WordPair("Mother", "Aunt"),
            new WordPair("Baby", "Child"),
            new WordPair("Boy", "Girl"),
            new WordPair("Man", "Woman"),
            new WordPair("Smile", "Face"),
            new WordPair("Laugh", "Smile"),
            new WordPair("Cry", "Tears"),
            new WordPair("Dream", "Sleep"),
            new WordPair("Sleep", "Bed"),
            new WordPair("Morning", "Breakfast"),
            new WordPair("Lunch", "Dinner"),
            new WordPair("Breakfast", "Coffee"),
            new WordPair("Dinner", "Restaurant"),
            new WordPair("Party", "Music"),
            new WordPair("Wedding", "Marriage"),
            new WordPair("Birthday", "Cake"),
            new WordPair("Christmas", "Santa"),
            new WordPair("Halloween", "Costume"),
            new WordPair("Festival", "Celebration"),
            new WordPair("Gift", "Surprise"),
            new WordPair("Balloon", "Party"),
            new WordPair("Candle", "Birthday"),
            new WordPair("Clock", "Time"),
            new WordPair("Watch", "Clock"),
            new WordPair("Calendar", "Date"),
            new WordPair("Phone", "Message"),
            new WordPair("Laptop", "Tablet"),
            new WordPair("Tablet", "Phone"),
            new WordPair("Headphones", "Earphones"),
            new WordPair("Speaker", "Radio"),
            new WordPair("Television", "Monitor"),
            new WordPair("Remote", "Controller"),
            new WordPair("Battery", "Charger"),
            new WordPair("Electricity", "Power"),
            new WordPair("Light", "Lamp"),
            new WordPair("Candle", "Lamp"),
            new WordPair("Torch", "Flashlight"),
            new WordPair("Fire", "Flame"),
            new WordPair("Water", "Rain"),
            new WordPair("Cloud", "Sky"),
            new WordPair("Sky", "Space"),
            new WordPair("Moon", "Star"),
            new WordPair("Earth", "World"),
            new WordPair("World", "Planet"),
            new WordPair("Galaxy", "Universe"),
            new WordPair("Space", "Universe"),
            new WordPair("Rocket", "Spacecraft"),
            new WordPair("Astronaut", "Cosmonaut"),
            new WordPair("Satellite", "Moon"),
            new WordPair("Star", "Comet"),
            new WordPair("Comet", "Meteor"),
            new WordPair("Meteor", "Asteroid"),
            new WordPair("Diamond", "Gold"),
            new WordPair("Gold", "Coin"),
            new WordPair("Money", "Wallet"),
            new WordPair("Coin", "Note"),
            new WordPair("Bank", "Money"),
            new WordPair("Shop", "Market"),
            new WordPair("Mall", "Market"),
            new WordPair("Restaurant", "Cafe"),
            new WordPair("Cafe", "Coffee"),
            new WordPair("Hotel", "Resort"),
            new WordPair("Hostel", "Dormitory"),
            new WordPair("Airport", "Terminal"),
            new WordPair("Station", "Platform"),
            new WordPair("Train", "Platform"),
            new WordPair("Bus", "Stop"),
            new WordPair("Road", "Bridge"),
            new WordPair("Tunnel", "Bridge"),
            new WordPair("Map", "GPS"),
            new WordPair("Compass", "GPS"),
            new WordPair("Key", "Password"),
            new WordPair("Lock", "Password"),
            new WordPair("Door", "Lock"),
            new WordPair("Window", "Curtain"),
            new WordPair("House", "Home"),
            new WordPair("Bedroom", "Living Room"),
            new WordPair("Kitchen", "Dining Room"),
            new WordPair("Bathroom", "Bedroom"),
            new WordPair("Sofa", "Chair"),
            new WordPair("Table", "Chair"),
            new WordPair("Desk", "Chair"),
            new WordPair("Bed", "Sofa"),
            new WordPair("Pillow", "Cushion"),
            new WordPair("Blanket", "Towel"),
            new WordPair("Carpet", "Rug"),
            new WordPair("Curtain", "Blind"),
            new WordPair("Fan", "Cooler"),
            new WordPair("Fridge", "Oven"),
            new WordPair("Microwave", "Oven"),
            new WordPair("Washing Machine", "Dishwasher"),
            new WordPair("Soap", "Detergent"),
            new WordPair("Shampoo", "Conditioner"),
            new WordPair("Towel", "Cloth"),
            new WordPair("Comb", "Brush"),
            new WordPair("Toothbrush", "Toothpaste"),
            new WordPair("Mirror", "Makeup"),
            new WordPair("Shirt", "T-Shirt"),
            new WordPair("Jacket", "Coat"),
            new WordPair("Shoes", "Boots"),
            new WordPair("Socks", "Shoes"),
            new WordPair("Jeans", "Shorts"),
            new WordPair("Dress", "Skirt"),
            new WordPair("Tie", "Bowtie"),
            new WordPair("Hat", "Helmet"),
            new WordPair("Glasses", "Goggles"),
            new WordPair("Watch", "Bracelet"),
            new WordPair("Ring", "Necklace"),
            new WordPair("Bag", "Backpack"),
            new WordPair("Wallet", "Bag"),
            new WordPair("Umbrella", "Tent"),
            new WordPair("Tent", "Camping"),
            new WordPair("Camping", "Hiking"),
            new WordPair("Hiking", "Trekking"),
            new WordPair("Mountain", "Hiking"),
            new WordPair("Forest", "Camping"),
            new WordPair("Beach", "Camping"),
            new WordPair("Fishing", "Swimming"),
            new WordPair("Fishing", "Boating"),
            new WordPair("Surfing", "Swimming"),
            new WordPair("Diving", "Snorkeling"),
            new WordPair("Skiing", "Snowboarding"),
            new WordPair("Skating", "Skateboarding"),
            new WordPair("Cycling", "Running"),
            new WordPair("Walking", "Running"),
            new WordPair("Jumping", "Running"),
            new WordPair("Football", "Soccer"),
            new WordPair("Cricket", "Baseball"),
            new WordPair("Basketball", "Netball"),
            new WordPair("Tennis", "Squash"),
            new WordPair("Badminton", "Tennis"),
            new WordPair("Golf", "Cricket"),
            new WordPair("Chess", "Puzzle"),
            new WordPair("Puzzle", "Riddle"),
            new WordPair("Game", "Competition"),
            new WordPair("Dice", "Cards"),
            new WordPair("Lottery", "Dice"),
            new WordPair("Magic", "Illusion"),
            new WordPair("Clown", "Circus"),
            new WordPair("Circus", "Festival"),
            new WordPair("Concert", "Festival"),
            new WordPair("Theater", "Cinema"),
            new WordPair("Cinema", "Netflix"),
            new WordPair("Actor", "Singer"),
            new WordPair("Celebrity", "Actor"),
            new WordPair("Famous", "Popular"),
            new WordPair("Hero", "Champion"),
            new WordPair("Villain", "Monster"),
            new WordPair("Police", "Security"),
            new WordPair("Soldier", "Guard"),
            new WordPair("Guard", "Watchman"),
            new WordPair("King", "President"),
            new WordPair("President", "Leader"),
            new WordPair("Leader", "Captain"),
            new WordPair("Captain", "Coach"),
            new WordPair("Judge", "Lawyer"),
            new WordPair("Court", "Prison"),
            new WordPair("Prison", "Jail"),
            new WordPair("Crime", "Mystery"),
            new WordPair("Detective", "Mystery"),
            new WordPair("Spy", "Agent"),
            new WordPair("Agent", "Detective"),
            new WordPair("Secret", "Mystery"),
            new WordPair("Password", "Secret"),
            new WordPair("Message", "Letter"),
            new WordPair("Letter", "Email"),
            new WordPair("Phone", "Radio"),
            new WordPair("Radio", "Television"),
            new WordPair("News", "Newspaper"),
            new WordPair("Newspaper", "Magazine"),
            new WordPair("Magazine", "Book"),
            new WordPair("Book", "Library"),
            new WordPair("Story", "Movie"),
            new WordPair("Novel", "Movie"),
            new WordPair("Poem", "Song"),
            new WordPair("Song", "Music"),
            new WordPair("Music", "Dance"),
            new WordPair("Dance", "Party"),
            new WordPair("Party", "Wedding"),
            new WordPair("Wedding", "Ring"),
            new WordPair("Ring", "Proposal"),
            new WordPair("Love", "Heart"),
            new WordPair("Heart", "Valentine"),
            new WordPair("Rose", "Valentine"),
            new WordPair("Chocolate", "Valentine"),
            new WordPair("Chocolate", "Cake"),
            new WordPair("Cake", "Birthday"),
            new WordPair("Birthday", "Party"),
            new WordPair("Christmas", "Gift"),
            new WordPair("Santa", "Reindeer"),
            new WordPair("Reindeer", "Deer"),
            new WordPair("Easter", "Egg"),
            new WordPair("Egg", "Chicken"),
            new WordPair("Farm", "Barn"),
            new WordPair("Barn", "Stable"),
            new WordPair("Horse", "Stable"),
            new WordPair("Cow", "Farm"),
            new WordPair("Sheep", "Farm"),
            new WordPair("Chicken", "Farm"),
            new WordPair("Farmer", "Tractor"),
            new WordPair("Tractor", "Truck"),
            new WordPair("Truck", "Trailer"),
            new WordPair("Factory", "Machine"),
            new WordPair("Machine", "Robot"),
            new WordPair("Robot", "Computer"),
            new WordPair("Computer", "Internet"),
            new WordPair("Internet", "Website"),
            new WordPair("Website", "Browser"),
            new WordPair("Browser", "Search"),
            new WordPair("Search", "Google"),
            new WordPair("Code", "Programming"),
            new WordPair("Java", "Python"),
            new WordPair("Android", "iPhone"),
            new WordPair("App", "Game"),
            new WordPair("Server", "Computer"),
            new WordPair("Database", "Storage"),
            new WordPair("Cloud", "Server"),
            new WordPair("Docker", "Container"),
            new WordPair("Git", "GitHub"),
            new WordPair("Developer", "Programmer"),
            new WordPair("Engineer", "Developer"),
            new WordPair("AI", "Robot"),
            new WordPair("Machine Learning", "AI"),
            new WordPair("Camera", "Lens"),
            new WordPair("Photo", "Selfie"),
            new WordPair("Video", "Movie"),
            new WordPair("Microphone", "Speaker"),
            new WordPair("Headphones", "Speaker"),
            new WordPair("Keyboard", "Mouse"),
            new WordPair("Laptop", "Computer"),
            new WordPair("Tablet", "Laptop"),
            new WordPair("Phone", "Tablet"),
            new WordPair("Charger", "Cable"),
            new WordPair("Battery", "Power"),
            new WordPair("Electricity", "Battery"),
            new WordPair("Solar", "Sun"),
            new WordPair("Windmill", "Fan"),
            new WordPair("Fire", "Sun"),
            new WordPair("Water", "Ice"),
            new WordPair("Ice", "Fridge"),
            new WordPair("Snow", "Winter"),
            new WordPair("Rain", "Monsoon"),
            new WordPair("Monsoon", "Cloud"),
            new WordPair("Storm", "Rain"),
            new WordPair("Rainbow", "Rain"),
            new WordPair("Lightning", "Storm"),
            new WordPair("Thunder", "Storm"),
            new WordPair("Fog", "Cloud"),
            new WordPair("Mist", "Fog"),
            new WordPair("Wind", "Storm"),
            new WordPair("Sunrise", "Morning"),
            new WordPair("Sunset", "Evening"),
            new WordPair("Night", "Moon"),
            new WordPair("Day", "Sun"),
            new WordPair("Star", "Night"),
            new WordPair("Dream", "Night"),
            new WordPair("Sleep", "Dream"),
            new WordPair("Alarm", "Clock"),
            new WordPair("Clock", "Alarm"),
            new WordPair("Time", "Clock"),
            new WordPair("Calendar", "Clock"),
            new WordPair("Monday", "Friday"),
            new WordPair("Weekend", "Holiday"),
            new WordPair("Summer", "Beach"),
            new WordPair("Winter", "Mountain"),
            new WordPair("Autumn", "Leaf"),
            new WordPair("Spring", "Flower"),
            new WordPair("Nature", "Forest"),
            new WordPair("Earth", "Nature"),
            new WordPair("Planet", "Earth"),
            new WordPair("Ocean", "Planet"),
            new WordPair("Space", "Astronaut"),
            new WordPair("Astronaut", "Rocket"),
            new WordPair("Rocket", "Launch"),
            new WordPair("Airport", "Flight"),
            new WordPair("Flight", "Plane"),
            new WordPair("Plane", "Pilot"),
            new WordPair("Pilot", "Airport"),
            new WordPair("Travel", "Tourist"),
            new WordPair("Tourist", "Camera"),
            new WordPair("Vacation", "Beach"),
            new WordPair("Holiday", "Vacation"),
            new WordPair("Passport", "Ticket"),
            new WordPair("Ticket", "Travel"),
            new WordPair("Hotel", "Booking"),
            new WordPair("Booking", "Reservation"),
            new WordPair("Restaurant", "Menu"),
            new WordPair("Menu", "Food"),
            new WordPair("Food", "Kitchen"),
            new WordPair("Chef", "Kitchen"),
            new WordPair("Waiter", "Restaurant"),
            new WordPair("Cashier", "Shop"),
            new WordPair("Customer", "Shop"),
            new WordPair("Customer", "Waiter"),
            new WordPair("Money", "Bank"),
            new WordPair("Bank", "ATM"),
            new WordPair("ATM", "Card"),
            new WordPair("Credit Card", "Wallet"),
            new WordPair("Wallet", "Money"),
            new WordPair("Coin", "Money"),
            new WordPair("Gold", "Treasure"),
            new WordPair("Treasure", "Pirate"),
            new WordPair("Pirate", "Ship"),
            new WordPair("Ship", "Ocean"),
            new WordPair("Ocean", "Beach"),
            new WordPair("Beach", "Sun"),
            new WordPair("Sun", "Summer"),
            new WordPair("Summer", "Ice Cream"),
            new WordPair("Ice Cream", "Chocolate"),
            new WordPair("Chocolate", "Candy"),
            new WordPair("Candy", "Lollipop"),
            new WordPair("Lollipop", "Stick"),
            new WordPair("Stick", "Tree"),
            new WordPair("Tree", "Forest"),
            new WordPair("Forest", "Mountain"),
            new WordPair("Mountain", "Snow"),
            new WordPair("Snow", "Skiing"),
            new WordPair("Skiing", "Winter"),
            new WordPair("Winter", "Coat"),
            new WordPair("Coat", "Jacket"),
            new WordPair("Jacket", "Shirt"),
            new WordPair("Shirt", "Jeans"),
            new WordPair("Jeans", "Shoes"),
            new WordPair("Shoes", "Socks"),
            new WordPair("Socks", "Foot"),
            new WordPair("Foot", "Shoe"),
            new WordPair("Hand", "Glove"),
            new WordPair("Glove", "Winter"),
            new WordPair("Hat", "Winter"),
            new WordPair("Scarf", "Winter"),
            new WordPair("Scarf", "Necklace"),
            new WordPair("Necklace", "Jewelry"),
            new WordPair("Jewelry", "Gold"),
            new WordPair("Gold", "Diamond"),
            new WordPair("Diamond", "Ring"),
            new WordPair("Ring", "Wedding"),
            new WordPair("Wedding", "Party"),
            new WordPair("Party", "Music"),
            new WordPair("Music", "Concert"),
            new WordPair("Concert", "Singer"),
            new WordPair("Singer", "Microphone"),
            new WordPair("Microphone", "Stage"),
            new WordPair("Stage", "Theater"),
            new WordPair("Theater", "Actor"),
            new WordPair("Actor", "Movie"),
            new WordPair("Movie", "Cinema"),
            new WordPair("Cinema", "Popcorn"),
            new WordPair("Popcorn", "Movie"),
            new WordPair("Movie", "Ticket"),
            new WordPair("Ticket", "Seat"),
            new WordPair("Seat", "Chair"),
            new WordPair("Chair", "Table"),
            new WordPair("Table", "Restaurant"),
            new WordPair("Restaurant", "Food"),
            new WordPair("Food", "Dinner"),
            new WordPair("Dinner", "Night"),
            new WordPair("Night", "Moon"),
            new WordPair("Moon", "Star"),
            new WordPair("Star", "Galaxy"),
            new WordPair("Galaxy", "Space"),
            new WordPair("Space", "Rocket"),
            new WordPair("Rocket", "Astronaut"),
            new WordPair("Astronaut", "Planet"),
            new WordPair("Planet", "Earth"),
            new WordPair("Earth", "World"),
            new WordPair("World", "Map"),
            new WordPair("Map", "Travel"),
            new WordPair("Travel", "Airport"),
            new WordPair("Airport", "Plane"),
            new WordPair("Plane", "Cloud"),
            new WordPair("Cloud", "Rain"),
            new WordPair("Rain", "Umbrella"),
            new WordPair("Umbrella", "Raincoat"),
            new WordPair("Raincoat", "Jacket"),
            new WordPair("Jacket", "Coat"),
            new WordPair("Coat", "Closet"),
            new WordPair("Closet", "Bedroom"),
            new WordPair("Bedroom", "Bed"),
            new WordPair("Bed", "Pillow"),
            new WordPair("Pillow", "Sleep"),
            new WordPair("Sleep", "Dream"),
            new WordPair("Dream", "Night"),
            new WordPair("Night", "Darkness"),
            new WordPair("Darkness", "Shadow"),
            new WordPair("Shadow", "Mirror"),
            new WordPair("Mirror", "Reflection"),
            new WordPair("Reflection", "Water"),
            new WordPair("Water", "River"),
            new WordPair("River", "Bridge"),
            new WordPair("Bridge", "Road"),
            new WordPair("Road", "Car"),
            new WordPair("Car", "Driver"),
            new WordPair("Driver", "Taxi"),
            new WordPair("Taxi", "Airport"),
            new WordPair("Airport", "Hotel"),
            new WordPair("Hotel", "Vacation"),
            new WordPair("Vacation", "Beach"),
            new WordPair("Beach", "Vacation"),
            new WordPair("Ocean", "Vacation"),
            new WordPair("Island", "Vacation"),
            new WordPair("Resort", "Beach"),
            new WordPair("Resort", "Hotel"),
            new WordPair("Hotel", "Pool"),
            new WordPair("Pool", "Swimming"),
            new WordPair("Swimming", "Water"),
            new WordPair("Water", "Pool"),
            new WordPair("Pool", "Summer"),
            new WordPair("Summer", "Sun"),
            new WordPair("Sun", "Beach"),
            new WordPair("Beach", "Sand"),
            new WordPair("Sand", "Desert"),
            new WordPair("Desert", "Camel"),
            new WordPair("Camel", "Horse"),
            new WordPair("Horse", "Racing"),
            new WordPair("Racing", "Car"),
            new WordPair("Car", "Race"),
            new WordPair("Race", "Track"),
            new WordPair("Track", "Running"),
            new WordPair("Running", "Marathon"),
            new WordPair("Marathon", "Medal"),
            new WordPair("Medal", "Trophy"),
            new WordPair("Trophy", "Winner"),
            new WordPair("Winner", "Champion"),
            new WordPair("Champion", "Player"),
            new WordPair("Player", "Team"),
            new WordPair("Team", "Coach"),
            new WordPair("Coach", "Referee"),
            new WordPair("Referee", "Whistle"),
            new WordPair("Whistle", "Police"),
            new WordPair("Police", "Uniform"),
            new WordPair("Uniform", "School"),
            new WordPair("School", "Student"),
            new WordPair("Student", "Book"),
            new WordPair("Book", "Exam"),
            new WordPair("Exam", "Result"),
            new WordPair("Result", "Score"),
            new WordPair("Score", "Game"),
            new WordPair("Game", "Player"),
            new WordPair("Player", "Controller"),
            new WordPair("Controller", "Console"),
            new WordPair("Console", "Game"),
            new WordPair("Game", "Fun"),
            new WordPair("Fun", "Party"),
            new WordPair("Party", "Friends"),
            new WordPair("Friends", "Family"),
            new WordPair("Family", "Home"),
            new WordPair("Home", "House"),
            new WordPair("House", "Garden"),
            new WordPair("Garden", "Flower"),
            new WordPair("Flower", "Bee"),
            new WordPair("Bee", "Honey"),
            new WordPair("Honey", "Tea"),
            new WordPair("Tea", "Coffee"),
            new WordPair("Coffee", "Cafe"),
            new WordPair("Cafe", "Restaurant"),
            new WordPair("Restaurant", "Dinner"),
            new WordPair("Dinner", "Food"),
            new WordPair("Food", "Kitchen"),
            new WordPair("Kitchen", "Chef"),
            new WordPair("Chef", "Recipe"),
            new WordPair("Recipe", "Book"),
            new WordPair("Book", "Story"),
            new WordPair("Story", "Adventure"),
            new WordPair("Adventure", "Journey"),
            new WordPair("Journey", "Travel"),
            new WordPair("Travel", "Passport"),
            new WordPair("Passport", "Airport"),
            new WordPair("Airport", "Flight"),
            new WordPair("Flight", "Cloud"),
            new WordPair("Cloud", "Sky"),
            new WordPair("Sky", "Star"),
            new WordPair("Star", "Moon"),
            new WordPair("Moon", "Night"),
            new WordPair("Night", "Dream"),
            new WordPair("Dream", "Adventure"),
            new WordPair("Adventure", "Mountain"),
            new WordPair("Mountain", "Hiking"),
            new WordPair("Hiking", "Forest"),
            new WordPair("Forest", "Nature"),
            new WordPair("Nature", "Earth"),
            new WordPair("Earth", "Planet"),
            new WordPair("Planet", "Space"),
            new WordPair("Space", "Galaxy"),
            new WordPair("Galaxy", "Universe"),
            new WordPair("Universe", "Stars"),
            new WordPair("Stars", "Night"),
            new WordPair("Night", "Moon"),
            new WordPair("Moon", "Reflection"),
            new WordPair("Reflection", "Mirror"),
            new WordPair("Mirror", "Glass"),
            new WordPair("Glass", "Window"),
            new WordPair("Window", "House"),
            new WordPair("House", "Home"),
            new WordPair("Home", "Family"),
            new WordPair("Family", "Dinner"),
            new WordPair("Dinner", "Table"),
            new WordPair("Table", "Chair"),
            new WordPair("Chair", "Room"),
            new WordPair("Room", "House"),
            new WordPair("House", "Door"),
            new WordPair("Door", "Key"),
            new WordPair("Key", "Lock"),
            new WordPair("Lock", "Security"),
            new WordPair("Security", "Police"),
            new WordPair("Police", "Detective"),
            new WordPair("Detective", "Mystery"),
            new WordPair("Mystery", "Puzzle"),
            new WordPair("Puzzle", "Game"),
            new WordPair("Game", "Fun"),
            new WordPair("Fun", "Laughter"),
            new WordPair("Laughter", "Smile"),
            new WordPair("Smile", "Happy"),
            new WordPair("Happy", "Party"),
            new WordPair("Party", "Celebration"),
            new WordPair("Celebration", "Festival"),
            new WordPair("Festival", "Music"),
            new WordPair("Music", "Dance"),
            new WordPair("Dance", "Stage"),
            new WordPair("Stage", "Actor"),
            new WordPair("Actor", "Movie"),
            new WordPair("Movie", "Story"),
            new WordPair("Story", "Book"),
            new WordPair("Book", "Library"),
            new WordPair("Library", "Study"),
            new WordPair("Study", "Exam"),
            new WordPair("Exam", "School"),
            new WordPair("School", "Teacher"),
            new WordPair("Teacher", "Student"),
            new WordPair("Student", "College"),
            new WordPair("College", "Campus"),
            new WordPair("Campus", "University"),
            new WordPair("University", "Degree"),
            new WordPair("Degree", "Career"),
            new WordPair("Career", "Job"),
            new WordPair("Job", "Office"),
            new WordPair("Office", "Computer"),
            new WordPair("Computer", "Keyboard"),
            new WordPair("Keyboard", "Mouse"),
            new WordPair("Mouse", "Computer"),
            new WordPair("Computer", "Internet"),
            new WordPair("Internet", "Cloud"),
            new WordPair("Cloud", "Server"),
            new WordPair("Server", "Database"),
            new WordPair("Database", "Data"),
            new WordPair("Data", "Information"),
            new WordPair("Information", "Knowledge"),
            new WordPair("Knowledge", "Book"),
            new WordPair("Book", "Education"),
            new WordPair("Education", "School"),
            new WordPair("School", "Future"),
            new WordPair("Future", "Dream"),
            new WordPair("Dream", "Goal"),
            new WordPair("Goal", "Success"),
            new WordPair("Success", "Winner"),
            new WordPair("Winner", "Trophy"),
            new WordPair("Trophy", "Medal"),
            new WordPair("Medal", "Competition"),
            new WordPair("Competition", "Game"),
            new WordPair("Game", "Challenge"),
            new WordPair("Challenge", "Adventure"),
            new WordPair("Adventure", "Travel"),
            new WordPair("Travel", "Journey"),
            new WordPair("Journey", "Road"),
            new WordPair("Road", "Highway"),
            new WordPair("Highway", "Car"),
            new WordPair("Car", "Traffic"),
            new WordPair("Traffic", "City"),
            new WordPair("City", "Building"),
            new WordPair("Building", "Office"),
            new WordPair("Office", "Work"),
            new WordPair("Work", "Career"),
            new WordPair("Career", "Success"),
            new WordPair("Success", "Money"),
            new WordPair("Money", "Bank"),
            new WordPair("Bank", "Card"),
            new WordPair("Card", "Wallet"),
            new WordPair("Wallet", "Pocket"),
            new WordPair("Pocket", "Jeans"),
            new WordPair("Jeans", "Shirt"),
            new WordPair("Shirt", "Jacket"),
            new WordPair("Jacket", "Shoes"),
            new WordPair("Shoes", "Walk"),
            new WordPair("Walk", "Park"),
            new WordPair("Park", "Tree"),
            new WordPair("Tree", "Nature"),
            new WordPair("Nature", "River"),
            new WordPair("River", "Water"),
            new WordPair("Water", "Ocean"),
            new WordPair("Ocean", "Wave"),
            new WordPair("Wave", "Surfing"),
            new WordPair("Surfing", "Beach"),
            new WordPair("Beach", "Sand"),
            new WordPair("Sand", "Castle"),
            new WordPair("Castle", "King"),
            new WordPair("King", "Crown"),
            new WordPair("Crown", "Queen"),
            new WordPair("Queen", "Palace"),
            new WordPair("Palace", "Castle"),
            new WordPair("Castle", "Knight"),
            new WordPair("Knight", "Sword"),
            new WordPair("Sword", "Shield"),
            new WordPair("Shield", "Armor"),
            new WordPair("Armor", "Soldier"),
            new WordPair("Soldier", "Army"),
            new WordPair("Army", "Battle"),
            new WordPair("Battle", "War"),
            new WordPair("War", "History"),
            new WordPair("History", "Museum"),
            new WordPair("Museum", "Art"),
            new WordPair("Art", "Painting"),
            new WordPair("Painting", "Artist"),
            new WordPair("Artist", "Gallery"),
            new WordPair("Gallery", "Museum"),
            new WordPair("Museum", "Tourist"),
            new WordPair("Tourist", "Camera"),
            new WordPair("Camera", "Photo"),
            new WordPair("Photo", "Memory"),
            new WordPair("Memory", "Story"),
            new WordPair("Story", "Friend"),
            new WordPair("Friend", "Smile"),
            new WordPair("Smile", "Happiness"),
            new WordPair("Happiness", "Life"),
            new WordPair("Life", "Adventure"),
            new WordPair("Adventure", "Dream"),
            new WordPair("Dream", "Future"),
            new WordPair("Future", "Hope"),
            new WordPair("Hope", "Success"),
            new WordPair("Success", "Goal"),
            new WordPair("Goal", "Achievement"),
            new WordPair("Achievement", "Trophy"),
            new WordPair("Trophy", "Champion"),
            new WordPair("Champion", "Victory"),
            new WordPair("Victory", "Celebration"),
            new WordPair("Celebration", "Party"),
            new WordPair("Party", "Friends"),
            new WordPair("Friends", "Game"),
            new WordPair("Game", "Laughter"),
            new WordPair("Laughter", "Fun"),
            new WordPair("Fun", "Childhood"),
            new WordPair("Childhood", "School"),
            new WordPair("School", "Memories"),
            new WordPair("Memories", "Photo"),
            new WordPair("Photo", "Camera"),
            new WordPair("Camera", "Vacation"),
            new WordPair("Vacation", "Travel"),
            new WordPair("Travel", "Adventure"),
            new WordPair("Adventure", "Mountain"),
            new WordPair("Mountain", "Forest"),
            new WordPair("Forest", "Wildlife"),
            new WordPair("Wildlife", "Safari"),
            new WordPair("Safari", "Jungle"),
            new WordPair("Jungle", "Tiger"),
            new WordPair("Tiger", "Lion"),
            new WordPair("Lion", "King"),
            new WordPair("King", "Crown"),
            new WordPair("Crown", "Gold"),
            new WordPair("Gold", "Treasure"),
            new WordPair("Treasure", "Pirate"),
            new WordPair("Pirate", "Adventure"),
            new WordPair("Adventure", "Ocean"),
            new WordPair("Ocean", "Ship"),
            new WordPair("Ship", "Captain"),
            new WordPair("Captain", "Sailor"),
            new WordPair("Sailor", "Boat"),
            new WordPair("Boat", "Fishing"),
            new WordPair("Fishing", "River"),
            new WordPair("River", "Fish"),
            new WordPair("Fish", "Aquarium"),
            new WordPair("Aquarium", "Zoo"),
            new WordPair("Zoo", "Animals"),
            new WordPair("Animals", "Forest"),
            new WordPair("Forest", "Trees"),
            new WordPair("Trees", "Leaves"),
            new WordPair("Leaves", "Autumn"),
            new WordPair("Autumn", "Rain"),
            new WordPair("Rain", "Monsoon"),
            new WordPair("Monsoon", "Umbrella"),
            new WordPair("Umbrella", "Weather"),
            new WordPair("Weather", "Forecast"),
            new WordPair("Forecast", "News"),
            new WordPair("News", "Newspaper"),
            new WordPair("Newspaper", "Morning"),
            new WordPair("Morning", "Breakfast"),
            new WordPair("Breakfast", "Coffee"),
            new WordPair("Coffee", "Cafe"),
            new WordPair("Cafe", "Friends"),
            new WordPair("Friends", "Chat"),
            new WordPair("Chat", "Phone"),
            new WordPair("Phone", "Message"),
            new WordPair("Message", "Notification"),
            new WordPair("Notification", "Phone"),
            new WordPair("Phone", "App"),
            new WordPair("App", "Game"),
            new WordPair("Game", "Impostor"),
            new WordPair("Impostor", "Detective"),
            new WordPair("Detective", "Mystery"),
            new WordPair("Mystery", "Secret"),
            new WordPair("Secret", "Password"),
            new WordPair("Password", "Lock"),
            new WordPair("Lock", "Key"),
            new WordPair("Key", "Door"),
            new WordPair("Door", "House"),
            new WordPair("House", "Home"),
            new WordPair("Home", "Family"),
            new WordPair("Family", "Love"),
            new WordPair("Love", "Heart"),
            new WordPair("Heart", "Life"),
            new WordPair("Life", "Adventure"),
            new WordPair("Adventure", "Journey"),
            new WordPair("Journey", "Story"),
            new WordPair("Story", "Memory"),
            new WordPair("Memory", "Photo"),
            new WordPair("Photo", "Moment"),
            new WordPair("Moment", "Time"),
            new WordPair("Time", "Clock"),
            new WordPair("Clock", "Watch"),
            new WordPair("Watch", "Screen"),
            new WordPair("Screen", "Phone"),
            new WordPair("Phone", "Camera"),
            new WordPair("Camera", "Lens"),
            new WordPair("Lens", "Glasses"),
            new WordPair("Glasses", "Vision"),
            new WordPair("Vision", "Dream"),
            new WordPair("Dream", "Goal"),
            new WordPair("Goal", "Future"),
            new WordPair("Future", "Career"),
            new WordPair("Career", "Success"),
            new WordPair("Success", "Achievement"),
            new WordPair("Achievement", "Victory"),
            new WordPair("Victory", "Champion"),
            new WordPair("Champion", "Trophy"),
            new WordPair("Trophy", "Prize"),
            new WordPair("Prize", "Gift"),
            new WordPair("Gift", "Surprise"),
            new WordPair("Surprise", "Party"),
            new WordPair("Party", "Birthday"),
            new WordPair("Birthday", "Cake"),
            new WordPair("Cake", "Dessert"),
            new WordPair("Dessert", "Ice Cream"),
            new WordPair("Ice Cream", "Summer"),
            new WordPair("Summer", "Beach"),
            new WordPair("Beach", "Ocean"),
            new WordPair("Ocean", "Vacation"),
            new WordPair("Vacation", "Hotel"),
            new WordPair("Hotel", "Travel"),
            new WordPair("Travel", "Airport"),
            new WordPair("Airport", "Passport"),
            new WordPair("Passport", "Country"),
            new WordPair("Country", "Map"),
            new WordPair("Map", "Compass"),
            new WordPair("Compass", "Direction"),
            new WordPair("Direction", "Road"),
            new WordPair("Road", "Journey"),
            new WordPair("Journey", "Adventure"),
            new WordPair("Adventure", "Experience"),
            new WordPair("Experience", "Memory"),
            new WordPair("Memory", "Story"),
            new WordPair("Story", "Book"),
            new WordPair("Book", "Knowledge"),
            new WordPair("Knowledge", "Education"),
            new WordPair("Education", "School"),
            new WordPair("School", "College"),
            new WordPair("College", "Career"),
            new WordPair("Career", "Future"),
            new WordPair("Future", "Dream"),
            new WordPair("Dream", "Hope"),
            new WordPair("Hope", "Happiness"),
            new WordPair("Happiness", "Smile"),
            new WordPair("Smile", "Friend"),
            new WordPair("Friend", "Family"),
            new WordPair("Family", "Home"),
            new WordPair("Home", "Peace"),
            new WordPair("Peace", "Nature"),
            new WordPair("Nature", "Forest"),
            new WordPair("Forest", "Mountain"),
            new WordPair("Mountain", "Adventure"),
            new WordPair("Adventure", "Travel"),
            new WordPair("Travel", "Beach"),
            new WordPair("Beach", "Sunset"),
            new WordPair("Sunset", "Evening"),
            new WordPair("Evening", "Dinner"),
            new WordPair("Dinner", "Restaurant"),
            new WordPair("Restaurant", "Chef"),
            new WordPair("Chef", "Food"),
            new WordPair("Food", "Taste"),
            new WordPair("Taste", "Flavor"),
            new WordPair("Flavor", "Spice"),
            new WordPair("Spice", "Curry"),
            new WordPair("Curry", "Rice"),
            new WordPair("Rice", "Biryani"),
            new WordPair("Biryani", "Restaurant"),
            new WordPair("Restaurant", "Friends"),
            new WordPair("Friends", "Party"),
            new WordPair("Party", "Music"),
            new WordPair("Music", "Dance"),
            new WordPair("Dance", "Fun"),
            new WordPair("Fun", "Game"),
            new WordPair("Game", "Competition"),
            new WordPair("Competition", "Winner"),
            new WordPair("Winner", "Champion"),
            new WordPair("Champion", "Victory"),
            new WordPair("Victory", "Celebration"),
            new WordPair("Celebration", "Festival"),
            new WordPair("Festival", "Culture"),
            new WordPair("Culture", "Tradition"),
            new WordPair("Tradition", "Family"),
            new WordPair("Family", "Home"),
            new WordPair("Home", "Memories"),
            new WordPair("Memories", "Childhood"),
            new WordPair("Childhood", "School"),
            new WordPair("School", "Friends"),
            new WordPair("Friends", "Playground"),
            new WordPair("Playground", "Park"),
            new WordPair("Park", "Garden"),
            new WordPair("Garden", "Flowers"),
            new WordPair("Flowers", "Spring"),
            new WordPair("Spring", "Rain"),
            new WordPair("Rain", "Rainbow"),
            new WordPair("Rainbow", "Sky"),
            new WordPair("Sky", "Cloud"),
            new WordPair("Cloud", "Storm"),
            new WordPair("Storm", "Thunder"),
            new WordPair("Thunder", "Lightning"),
            new WordPair("Lightning", "Electricity"),
            new WordPair("Electricity", "Power"),
            new WordPair("Power", "Energy"),
            new WordPair("Energy", "Battery"),
            new WordPair("Battery", "Charger"),
            new WordPair("Charger", "Phone"),
            new WordPair("Phone", "Technology"),
            new WordPair("Technology", "Computer"),
            new WordPair("Computer", "Internet"),
            new WordPair("Internet", "World"),
            new WordPair("World", "Earth"),
            new WordPair("Earth", "Nature"),
            new WordPair("Nature", "Life"),
            new WordPair("Life", "Adventure")
    );


    // ---------------------------------------------------------
    // ROOM
    // ---------------------------------------------------------

    public GameRoom createRoom() {

        String roomId = generateRoomId();

        GameRoom room = new GameRoom(roomId);

        rooms.put(roomId, room);

        return room;
    }


    public GameRoom getRoom(String roomId) {

        return rooms.get(roomId);
    }


    // ---------------------------------------------------------
    // SESSION / PLAYER
    // ---------------------------------------------------------

    public void addSession(WebSocketSession session) {

        sessions.put(
                session.getId(),
                session
        );
    }


    public WebSocketSession getSession(
            String sessionId) {

        return sessions.get(sessionId);
    }


    public Player createPlayer(
            String sessionId,
            String name) {

        Player player = new Player();

        player.setId(sessionId);
        player.setName(name);

        players.put(
                sessionId,
                player
        );

        return player;
    }


    public Player getPlayer(String sessionId) {

        return players.get(sessionId);
    }


    // ---------------------------------------------------------
    // JOIN ROOM
    // ---------------------------------------------------------

    public boolean joinRoom(
            String roomId,
            String sessionId) {

        GameRoom room = rooms.get(roomId);

        if (room == null) {
            return false;
        }

        Player player = players.get(sessionId);

        if (player == null) {
            return false;
        }

        player.setRoomId(roomId);
        // Assign public player ID
        player.setPublicId("P" + (room.getPlayers().size() + 1));
        room.addPlayer(player);

        // Initially everyone is alive
        room.getActivePlayers().add(player);

        return true;
    }


    // ---------------------------------------------------------
    // BROADCAST
    // ---------------------------------------------------------

    public void broadcastToRoom(
            String roomId,
            String message)
            throws IOException {

        GameRoom room = rooms.get(roomId);

        if (room == null) {
            return;
        }

        // Use players instead of activePlayers.
        // Eliminated players can still see the game.
        for (Player player : room.getPlayers()) {

            WebSocketSession session =
                    sessions.get(player.getId());

            if (session != null &&
                    session.isOpen()) {

                session.sendMessage(
                        new TextMessage(message)
                );
            }
        }
    }


    public void sendToPlayer(
            String playerId,
            String message)
            throws IOException {

        WebSocketSession session =
                sessions.get(playerId);

        if (session != null &&
                session.isOpen()) {

            session.sendMessage(
                    new TextMessage(message)
            );
        }
    }


    // ---------------------------------------------------------
    // PLAYER LIST
    // ---------------------------------------------------------

    public String getPlayerList(
            String roomId) {

        GameRoom room = rooms.get(roomId);

        if (room == null) {
            return "";
        }

        StringBuilder result =
                new StringBuilder();

        for (Player player :
                room.getPlayers()) {

            if (result.length() > 0) {
                result.append(",");
            }

            result.append(player.getPublicId())
                    .append(":")
                    .append(player.getName());
        }

        return result.toString();
    }


    // ---------------------------------------------------------
    // WORD
    // ---------------------------------------------------------

    public WordPair selectWordPair() {

        int randomIndex =
                (int) (
                        Math.random() *
                                wordPairs.size()
                );

        return wordPairs.get(randomIndex);
    }


    // ---------------------------------------------------------
    // IMPOSTER
    // ---------------------------------------------------------

    public Player selectImposter(
            String roomId) {

        GameRoom room =
                rooms.get(roomId);

        if (room == null ||
                room.getPlayers().isEmpty()) {

            return null;
        }

        List<Player> roomPlayers =
                room.getPlayers();

        int randomIndex =
                (int) (
                        Math.random() *
                                roomPlayers.size()
                );

        Player imposter =
                roomPlayers.get(randomIndex);

        room.setImposterId(
                imposter.getId()
        );

        return imposter;
    }


    // ---------------------------------------------------------
    // START GAME
    // ---------------------------------------------------------

    public void startGame(
            String roomId)
            throws IOException {

        GameRoom room =
                rooms.get(roomId);

        if (room == null) {
            return;
        }

        if (room.getPlayers().size() < 3) {
            return;
        }

        // Game starts in describing phase
        room.setGamePhase(
                GamePhase.DESCRIBING
        );


        // Select imposter
        Player imposter =
                selectImposter(roomId);


        // Select word pair
        WordPair pair =
                selectWordPair();


        room.setNormalWord(
                pair.getNormalWord()
        );

        room.setImposterWord(
                pair.getImposterWord()
        );


        // Select first active describer
        Player firstDescriber =
                selectFirstActiveDescriber(
                        roomId
                );


        if (firstDescriber != null) {

            broadcastToRoom(
                    roomId,
                    "YOUR_TURN:" +
                            firstDescriber.getPublicId()
            );
        }


        room.setGameStarted(true);


        // Give every player their word
        for (Player player :
                room.getPlayers()) {

            String word;

            if (player.getId().equals(
                    room.getImposterId())) {

                word =
                        room.getImposterWord();

            } else {

                word =
                        room.getNormalWord();
            }

            sendToPlayer(
                    player.getId(),
                    "YOUR_WORD:" + word
            );
        }


        System.out.println(
                "Game started in room: " +
                        roomId
        );

        System.out.println(
                "Imposter: " +
                        imposter.getName()
        );

        System.out.println(
                "Normal word: " +
                        pair.getNormalWord()
        );

        System.out.println(
                "Imposter word: " +
                        pair.getImposterWord()
        );
    }


    // ---------------------------------------------------------
    // SELECT FIRST ACTIVE DESCRIBER
    // ---------------------------------------------------------

    public Player selectFirstActiveDescriber(
            String roomId) {

        GameRoom room =
                rooms.get(roomId);

        if (room == null ||
                room.getActivePlayers().isEmpty()) {

            return null;
        }


        List<Player> activePlayers =
                room.getActivePlayers();


        int randomIndex =
                (int) (
                        Math.random() *
                                activePlayers.size()
                );


        Player player =
                activePlayers.get(randomIndex);


        room.setCurrentDescriberId(
                player.getId()
        );

        return player;
    }


    // ---------------------------------------------------------
    // DESCRIPTION
    // ---------------------------------------------------------

    public void submitDescription(
            String playerId,
            String description)
            throws IOException {

        Player player =
                players.get(playerId);

        if (player == null) {
            return;
        }


        String roomId =
                player.getRoomId();


        GameRoom room =
                rooms.get(roomId);


        if (room == null) {
            return;
        }


        // Must be describing phase
        if (room.getGamePhase()
                != GamePhase.DESCRIBING) {

            sendToPlayer(
                    playerId,
                    "Description phase is over"
            );

            return;
        }


        // Must be alive
        if (!room.getActivePlayers()
                .contains(player)) {

            sendToPlayer(
                    playerId,
                    "You are eliminated"
            );

            return;
        }


        // Must be current describer
        if (!room.getCurrentDescriberId()
                .equals(playerId)) {

            sendToPlayer(
                    playerId,
                    "Not your turn"
            );

            return;
        }


        // Prevent duplicate description
        if (room.getDescribed()
                .contains(player)) {

            sendToPlayer(
                    playerId,
                    "You already described"
            );

            return;
        }


        // Store description
        room.getDesc().put(
                player,
                description
        );

        room.getDescribed().add(player);


        // Immediately broadcast description
        broadcastToRoom(
                roomId,
                player.getName() +
                        " : " +
                        description
        );


        // -----------------------------------------------------
        // Everyone alive has described
        // -----------------------------------------------------

        if (room.getDescribed().size()
                == room.getActivePlayers().size()) {

            meeting(roomId);

            return;
        }


        // -----------------------------------------------------
        // Find next active player
        // -----------------------------------------------------

        List<Player> activePlayers =
                room.getActivePlayers();


        int idx =
                findPlayerIndex(
                        activePlayers,
                        playerId
                );


        int nextIndex =
                (idx + 1)
                        % activePlayers.size();


        Player nextPlayer =
                activePlayers.get(nextIndex);


        room.setCurrentDescriberId(
                nextPlayer.getId()
        );


        broadcastToRoom(
                roomId,
                "YOUR_TURN:" +
                        nextPlayer.getPublicId()
        );
    }


    // ---------------------------------------------------------
    // FIND PLAYER INDEX
    // ---------------------------------------------------------

    private int findPlayerIndex(
            List<Player> players,
            String playerId) {

        for (int i = 0;
             i < players.size();
             i++) {

            if (players.get(i)
                    .getId()
                    .equals(playerId)) {

                return i;
            }
        }

        return -1;
    }


    // ---------------------------------------------------------
    // MEETING
    // ---------------------------------------------------------

    private void meeting(
            String roomId)
            throws IOException {

        GameRoom room =
                rooms.get(roomId);

        if (room == null) {
            return;
        }


        room.setGamePhase(
                GamePhase.MEETING
        );


        // Reset votes
        room.getVotes().clear();


        // Result has not been processed
        room.setMeetingResultProcessed(
                false
        );


        broadcastToRoom(
                roomId,
                "MEETING_STARTED"
        );


        broadcastToRoom(
                roomId,
                "Please Vote - 45 seconds"
        );


        // -----------------------------------------------------
        // Start 45 second timer
        // -----------------------------------------------------

        ScheduledFuture<?> future =
                scheduler.schedule(
                        () -> {

                            try {

                                // Players who didn't vote
                                // automatically SKIP
                                addMissingVotesAsSkip(
                                        roomId
                                );

                                calculateMeetingResult(
                                        roomId
                                );

                            } catch (IOException e) {

                                e.printStackTrace();
                            }

                        },
                        45,
                        TimeUnit.SECONDS
                );


        room.setMeetingTimer(
                future
        );
    }


    // ---------------------------------------------------------
    // MISSING VOTES = SKIP
    // ---------------------------------------------------------

    private void addMissingVotesAsSkip(
            String roomId) {

        GameRoom room =
                rooms.get(roomId);

        if (room == null) {
            return;
        }


        for (Player player :
                room.getActivePlayers()) {

            room.getVotes().putIfAbsent(
                    player.getId(),
                    "SKIP"
            );
        }
    }


    // ---------------------------------------------------------
    // SUBMIT VOTE
    // ---------------------------------------------------------

    public void submitVote(
            String playerId,
            String targetId)
            throws IOException {

        Player player =
                players.get(playerId);

        if (player == null) {
            return;
        }


        String roomId =
                player.getRoomId();


        GameRoom room =
                rooms.get(roomId);


        if (room == null) {
            return;
        }


        // -----------------------------------------------------
        // Must be meeting phase
        // -----------------------------------------------------

        if (room.getGamePhase()
                != GamePhase.MEETING) {

            sendToPlayer(
                    playerId,
                    "Voting is not active"
            );

            return;
        }


        // -----------------------------------------------------
        // Player must be alive
        // -----------------------------------------------------

        if (!room.getActivePlayers()
                .contains(player)) {

            sendToPlayer(
                    playerId,
                    "You are eliminated"
            );

            return;
        }


        // -----------------------------------------------------
        // One vote only
        // -----------------------------------------------------

        if (room.getVotes()
                .containsKey(playerId)) {

            sendToPlayer(
                    playerId,
                    "You already voted"
            );

            return;
        }


        // -----------------------------------------------------
        // Validate target
        // -----------------------------------------------------
        if (targetId == null || targetId.isBlank()) {
            sendToPlayer(playerId, "Invalid vote");
            return;
        }


        if (!targetId.equals("SKIP")) {

            Player target = room.getActivePlayers()
                    .stream()
                    .filter(p -> p.getPublicId().equals(targetId))
                    .findFirst()
                    .orElse(null);


            if (target == null ||
                    !room.getActivePlayers()
                            .contains(target)) {

                sendToPlayer(
                        playerId,
                        "Invalid target"
                );

                return;
            }
        }


        // -----------------------------------------------------
        // Store vote
        // -----------------------------------------------------

        room.getVotes().put(
                playerId,
                targetId
        );


        broadcastToRoom(
                roomId,
                player.getName() +
                        " voted"
        );


        // -----------------------------------------------------
        // Everyone alive voted?
        // -----------------------------------------------------

        if (room.getVotes().size()
                == room.getActivePlayers().size()) {


            // Cancel 45-second timer
            ScheduledFuture<?> timer =
                    room.getMeetingTimer();


            if (timer != null) {

                timer.cancel(false);

                room.setMeetingTimer(null);
            }


            calculateMeetingResult(
                    roomId
            );
        }
    }


    // ---------------------------------------------------------
    // CALCULATE MEETING RESULT
    // ---------------------------------------------------------

    private void calculateMeetingResult(
            String roomId)
            throws IOException {

        GameRoom room =
                rooms.get(roomId);


        if (room == null) {
            return;
        }


        // -----------------------------------------------------
        // Prevent timer + last vote from running twice
        // -----------------------------------------------------

        synchronized (room) {

            if (room.isMeetingResultProcessed()) {
                return;
            }

            room.setMeetingResultProcessed(
                    true
            );
        }


        // -----------------------------------------------------
        // Count votes
        // -----------------------------------------------------

        Map<String, Integer> voteCount =
                new HashMap<>();


        for (String targetId :
                room.getVotes().values()) {

            voteCount.put(
                    targetId,
                    voteCount.getOrDefault(
                            targetId,
                            0
                    ) + 1
            );
        }


        // -----------------------------------------------------
        // Find maximum votes
        // -----------------------------------------------------

        int maxVotes = 0;

        int numberOfPlayersWithMaxVotes = 0;

        String eliminatedId = null;


        for (Map.Entry<String, Integer> entry :
                voteCount.entrySet()) {

            int count =
                    entry.getValue();


            if (count > maxVotes) {

                maxVotes = count;

                numberOfPlayersWithMaxVotes = 1;

                eliminatedId =
                        entry.getKey();

            } else if (count == maxVotes) {

                numberOfPlayersWithMaxVotes++;
            }
        }


        // -----------------------------------------------------
        // Nobody voted / all somehow skipped
        // -----------------------------------------------------

        if (maxVotes == 0) {

            broadcastToRoom(
                    roomId,
                    "Nobody voted - Next round"
            );

            startNextRound(roomId);

            return;
        }


        // -----------------------------------------------------
        // Tie
        // -----------------------------------------------------

        if (numberOfPlayersWithMaxVotes > 1) {

            broadcastToRoom(
                    roomId,
                    "Tie - Nobody eliminated"
            );

            startNextRound(roomId);

            return;
        }


        // -----------------------------------------------------
        // SKIP has highest votes
        // -----------------------------------------------------

        if (eliminatedId.equals("SKIP")) {

            broadcastToRoom(
                    roomId,
                    "SKIP - Nobody eliminated"
            );

            startNextRound(roomId);

            return;
        }


        // -----------------------------------------------------
        // Eliminate player
        // -----------------------------------------------------

        String finalEliminatedId = eliminatedId;
        Player eliminated = room.getActivePlayers()
                .stream()
                .filter(p -> p.getPublicId().equals(finalEliminatedId))
                .findFirst()
                .orElse(null);


        if (eliminated == null) {
            return;
        }


        room.getActivePlayers()
                .remove(eliminated);


        broadcastToRoom(
                roomId,
                eliminated.getName() +
                        " was eliminated"
        );


        // -----------------------------------------------------
        // Imposter eliminated
        // -----------------------------------------------------

        if (eliminated.getId()
                .equals(room.getImposterId())) {

            room.setGamePhase(
                    GamePhase.FINISHED
            );


            broadcastToRoom(
                    roomId,
                    "NORMAL PLAYERS WIN!"
            );

            return;
        }


        // -----------------------------------------------------
        // Only two players remain
        // -----------------------------------------------------

        if (room.getActivePlayers()
                .size() == 2) {

            room.setGamePhase(
                    GamePhase.FINISHED
            );


            broadcastToRoom(
                    roomId,
                    "IMPOSTER WINS!"
            );

            return;
        }


        // -----------------------------------------------------
        // Continue to next round
        // -----------------------------------------------------

        startNextRound(roomId);
    }


    // ---------------------------------------------------------
    // NEXT ROUND
    // ---------------------------------------------------------

    private void startNextRound(
            String roomId)
            throws IOException {

        GameRoom room =
                rooms.get(roomId);


        if (room == null) {
            return;
        }


        // -----------------------------------------------------
        // Clear previous round
        // -----------------------------------------------------

        room.getDescribed().clear();

        room.getDesc().clear();

        room.getVotes().clear();

        room.setMeetingResultProcessed(
                false
        );


        // -----------------------------------------------------
        // Select random active player
        // -----------------------------------------------------

        Player first =
                selectFirstActiveDescriber(
                        roomId
                );


        if (first == null) {
            return;
        }


        room.setGamePhase(
                GamePhase.DESCRIBING
        );


        broadcastToRoom(
                roomId,
                "NEXT_ROUND"
        );


        broadcastToRoom(
                roomId,
                "Your Turn : " +
                        first.getName()
        );
    }


    // ---------------------------------------------------------
    // ROOM ID
    // ---------------------------------------------------------

    private String generateRoomId() {

        return String.valueOf(
                (int) (
                        Math.random() * 9000
                ) + 1000
        );
    }
}
#convert character set to UTF-8
ALTER TABLE table_name CONVERT TO CHARACTER SET utf8;

# Ads  Storage Table
create table ads (
    text varchar(300) not null,
    category varchar(100) not null,	
    keywords varchar(100) not null,	
    campaignID varchar(50) primary key,
    status varchar(10) not null,
    targetGender varchar(3) not null,
    targetAgeStart TINYINT not null,
    targetAgeEnd TINYINT not null,
    targetCity varchar(20) not null,	
    targetState varchar(20) not null,	
    targetCountry varchar(20) not null,
    targetIncomeBucket varchar(5) not null,
    targetDevice varchar(20) not null,
    cpc	 double not null,
    cpa  double not null,
    cpm  double not null,	
    budget double not null,
    currentSlotBudget double not null,
    dateRangeStart date not null,
    dateRangeEnd date not null,
    timeRangeStart time not null,
    timeRangeEnd time not null
);

# Ads serve Table

create table served_ads(
    requestID varchar(50) primary key,
    campaignID varchar(50) not null,
    userID varchar(50) not null,
    auctionCPM decimal(6,6) not null,
    auctionCPC decimal(6,6) not null,
    auctionCPA decimal(6,6) not null,
    targetAgeRange varchar(5) not null,
    targetLocation varchar(50) not null,
    targetGender varchar(3) not null,
    targetIncomeBucket varchar(5) not null,
    targetDeviceType varchar(20) not null,
    campaignStartTime DATETIME not null,
    campaignEndTime DATETIME not null,
    userFeedbackTimeStamp TIMESTAMP not null
);

# User table

create table user(
    userID varchar(50) primary key,
    age TINYINT not null,
    gender varchar(1) not null,
    internetUsage varchar(50),
    incomeBucket varchar(3),
    userAgentString varchar(300),
    deviceType varchar(50),
    Websites varchar(300),
    Movies varchar(300),
    Music varchar(300),
    Program  varchar(300),
    Books varchar(300),
    Negatives varchar(300),
    Positives varchar(300),
);

# loading data into User Table
load data local infile 'users_500k.csv'
 into table user
 fields terminated by '|'
 lines terminated by '\n'
 IGNORE 1 LINES;
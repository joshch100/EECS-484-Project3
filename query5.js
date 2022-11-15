// Query 5
// Find the oldest friend for each user who has a friend. For simplicity,
// use only year of birth to determine age, if there is a tie, use the
// one with smallest user_id. You may find query 2 and query 3 helpful.
// You can create selections if you want. Do not modify users collection.
// Return a javascript object : key is the user_id and the value is the oldest_friend id.
// You should return something like this (order does not matter):
// {user1:userx1, user2:userx2, user3:userx3,...}

function oldest_friend(dbname) {
    db = db.getSiblingDB(dbname);

    let results = {};
    // TODO: implement oldest friends
    db.createCollection("flat_users");

    db.users.aggregate(
        { $project : {
            "_id": 0 ,
            user_id : 1 ,
            friends : 1
        }},
        { $unwind : "$friends" },
        { $out: "flat_users" }
    );

    //fliiping the flat_users table and inesert into the orginal table
    db.flat_users.find().forEach(function(doc) {
        db.flat_users.insert({"user_id": doc.friends, "friends": doc.user_id});
    });


    //create an array of birthyears and sort them in ascending order 
    var yearob = {};
    db.users.find().sort({"YOB":1}).forEach(function(doc) {yearob[doc.user_id] = doc.YOB;});


    db.flat_users.aggregate({$group : {_id : "$user_id", friends: { $push: "$friends" }}}).forEach(function(user){
        //iterate through the _id and find the oldest friend id
        var userid = user._id;
        var oldestFriendid = user.friends[0] //intialize to the first one
        var year = yearob[oldestFriendid] //intialize to the first friend's year

        //loop through the friends array
        for (i = 0; i < user.friends.length; i++){
            if(yearob[user.friends[i]] < year){
                year = yearob[user.friends[i]];
                oldestFriendid = user.friends[i];
            }
            //in case of a tie, return the friend with the smallest id
            else if(yearob[user.friends[i]] == year){
                if(user.friends[i] < oldestFriendid){
                    oldestFriendid = user.friends[i];
                }
            }
        }
        results[userid] = oldestFriendid;

    });
    return results;
}

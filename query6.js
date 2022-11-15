// Query 6
// Find the average friend count per user.
// Return a decimal value as the average user friend count of all users in the users collection.

function find_average_friendcount(dbname) {
    db = db.getSiblingDB(dbname);

    // TODO: calculate the average friend count

    //Create flat_users
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

    var friendavg = db.flat_users.find().count()/parseFloat(db.users.find().count());

    return friendavg;
}

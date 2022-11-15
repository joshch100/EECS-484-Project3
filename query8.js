// Query 8
// Find the city average friend count per user using MapReduce.

let city_average_friendcount_mapper = function () {
    // TODO: Implement the map function
    var usercount = 1;
    var friendcount = this.friends.length;
    emit(this.hometown.city, {user_count: usercount, numoffriends: friendcount});

};

let city_average_friendcount_reducer = function (key, values) {
    // TODO: Implement the reduce function
    var reducerval = {user_count: 0, numoffriends: 0};
    for (var i = 0; i < values.length; i++){
        reducerval.user_count += values[i].user_count;
        reducerval.numoffriends += values[i].numoffriends;
    }

    return reducerval;
};

let city_average_friendcount_finalizer = function (key, reduceVal) {
    // We've implemented a simple forwarding finalize function. This implementation
    // is naive: it just forwards the reduceVal to the output collection.
    // TODO: Feel free to change it if needed.
    finalval = reduceVal.numoffriends/parseFloat(reduceVal.user_count);

    return finalval;
};

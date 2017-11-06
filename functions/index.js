/**
 * Copyright 2016 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';

const functions = require('firebase-functions');
const bigquery = require('@google-cloud/bigquery')();

/**
 * Writes all logs from the Realtime Database into bigquery.
 */

/**
exports.addtobigquery = functions.database.ref('/logs/{logid}').onWrite(event => {
  // TODO: Make sure you set the `bigquery.datasetName` Google Cloud environment variable.
  const dataset = bigquery.dataset(functions.config().bigquery.datasetname);
  // TODO: Make sure you set the `bigquery.tableName` Google Cloud environment variable.
  const table = dataset.table('logs');

  return table.insert({
    ID: event.data.key,
    MESSAGE: event.data.val().message,
    NUMBER: event.data.val().number
  });
});
**/

exports.copyreviewstobigquery = functions.firestore
  .document('restaurants/{restaurantid}/ratings/{ratingid}')
  .onCreate(event => {

    const dataset = bigquery.dataset(functions.config().bigquery.datasetname);
    // TODO: Make sure you set the `bigquery.tableName` Google Cloud environment variable.
    const table = dataset.table('ratings');

    // Get an object representing the document
    // e.g. {'name': 'Marie', 'age': 66}
    var ratingDocSnap = event.data.data();

    // insert into a simple table
    var bqrow = table.insert({
      rating: ratingDocSnap.rating,
      text: ratingDocSnap.text,
      timestamp: ratingDocSnap.timestamp,
      userId: ratingDocSnap.userId,
      userName: ratingDocSnap.userName
    });

   	// insert into a flattened table using results of parent restaurant.
   	// as it happens, this lives at (root-ward) path:  DocumentReference ^ CollectionReference ^ DocumentReference ...

/*
    console.log('Raw event: ', event);
    console.log('Raw event.data: ', event.data);
    console.log('Raw event.data.ref: ', event.data.ref);
    console.log('Raw event.data.ref.parent: ', event.data.ref.parent);
    console.log('Raw event.data.ref.parent.parent: ', event.data.ref.parent.parent);
	// This gives back "...get():  Promise { <pending> }" so yeah
    console.log('Raw event.data.ref.parent.parent.get(): ', event.data.ref.parent.parent.get());
*/

	const supertable = dataset.table('restaurant_ratings');

    let getParentRestDoc = event.data.ref.parent.parent.get();
    getParentRestDoc.then(function(parentRestDocResult) {

    	var parentRestDocSnap = parentRestDocResult.data();

    	console.log('Rating data: ', ratingDocSnap);
    	console.log('Restaurant data: ', parentRestDocSnap);

    	return supertable.insert({
	      restaurant_city: parentRestDocSnap.city,
	      restaurant_name: parentRestDocSnap.name,
	      restaurant_category: parentRestDocSnap.category,
	      restaurant_price: parentRestDocSnap.price,
	      rating: ratingDocSnap.rating,
    	  text: ratingDocSnap.text,
	      timestamp: ratingDocSnap.timestamp,
	      userId: ratingDocSnap.userId,
	      userName: ratingDocSnap.userName
	    });
    });

    return bqrow;
});

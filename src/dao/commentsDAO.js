import { ObjectId } from "bson"

let comments

export default class CommentsDAO {
  static async injectDB(conn) {
    if (comments) {
      return
    }
    try {
      comments = await conn.db(process.env.MFLIX_NS).collection("comments")
    } catch (e) {
      console.error(`Unable to establish collection handles in userDAO: ${e}`)
    }
  }

  /**
  Ticket: Create/Update Comments

  For this ticket, you will need to implement the following two methods:

  - addComment
  - updateComment

  You can find these methods below this docstring. Make sure to read the comments
  to better understand the task.
  */

  static async addComment(movieId, user, comment, date) {
    try {
      // here's how the commentDoc is constructed
      const commentDoc = {
        name: user.name,
        email: user.email,
        movie_id: ObjectId(movieId),
        text: comment,
        date: date,
      }
  
      return await comments.insertOne(commentDoc)
    } catch (e) {
      console.error(`Unable to post comment: ${e}`)
      return { error: e }
    }
  }
  
  static async updateComment(commentId, userEmail, text, date) {
    try {
      // here's how the update is performed
      const updateResponse = await comments.updateOne(
        { _id: ObjectId(commentId), email: userEmail },
        { $set: { text, date } },
      )
  
      return updateResponse
    } catch (e) {
      console.error(`Unable to update comment: ${e}`)
      return { error: e }
    }
  }

  static async deleteComment(commentId, userEmail) {
    const deleteResponse = await comments.deleteOne({
      _id: ObjectId(commentId),
      // the user's email is passed here to make sure they own the comment
      email: userEmail,
    })
  
    return deleteResponse
  }

  static async mostActiveCommenters() {
    /**
    Ticket: User Report

    Build a pipeline that returns the 20 most frequent commenters on the MFlix
    site. You can do this by counting the number of occurrences of a user's
    email in the `comments` collection.
    */
    try {
      // TODO Ticket: User Report
      // Return the 20 users who have commented the most on MFlix.
      const pipeline = [
        { $group: { _id: '$email', count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 20 }
      ]

      // TODO Ticket: User Report
      // Use a more durable Read Concern here to make sure this data is not stale.
      const readConcern = "majority";

      const aggregateResult = await comments.aggregate(pipeline, {
        readConcern,
      })

      return await aggregateResult.toArray()
    } catch (e) {
      console.error(`Unable to retrieve most active commenters: ${e}`)
      return { error: e }
    }
  }
}

/**
 * Success/Error return object
 * @typedef DAOResponse
 * @property {boolean} [success] - Success
 * @property {string} [error] - Error
 */

package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import io.jsonwebtoken.IncorrectClaimException;
import mflix.api.models.Session;
import mflix.api.models.User;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.text.MessageFormat;
import java.util.Map;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Configuration
public class UserDao extends AbstractMFlixDao {

    private final MongoCollection<User> usersCollection;
    //TODO> Ticket: User Management - do the necessary changes so that the sessions collection
    //returns a Session object
    private final MongoCollection<Session> sessionsCollection;

    private final Logger log;
    public final static String SESSIONS_COLLECTION= "sessions";
    public final static String USERS_COLLECTION= "users";

    @Autowired
    public UserDao(
            MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
        super(mongoClient, databaseName);
        CodecRegistry pojoCodecRegistry =
                fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        usersCollection = db.getCollection(USERS_COLLECTION, User.class).withCodecRegistry(pojoCodecRegistry);
        log = LoggerFactory.getLogger(this.getClass());
        //TODO> Ticket: User Management - implement the necessary changes so that the sessions
        // collection returns a Session objects instead of Document objects.
        sessionsCollection = db.getCollection(SESSIONS_COLLECTION, Session.class).withCodecRegistry(pojoCodecRegistry);
        usersCollection.withWriteConcern(WriteConcern.MAJORITY);
    }

    /**
     * Inserts the `user` object in the `users` collection.
     *
     * @param user - User object to be added
     * @return True if successful, throw IncorrectDaoOperation otherwise
     */
    public boolean addUser(User user) {
        //TODO > Ticket: Durable Writes -  you might want to use a more durable write concern here!
        if(getUser(user.getEmail()) == null) {
        	
        	usersCollection.insertOne(user);
        } else {
        	log.warn("The user already exist");
        	return false;
        }
    	
        return true;
        //TODO > Ticket: Handling Errors - make sure to only add new users
        // and not users that already exist.

    }

    /**
     * Creates session using userId and jwt token.
     *
     * @param userId - user string identifier
     * @param jwt    - jwt string token
     * @return true if successful
     */
    public boolean createUserSession(String userId, String jwt) {
        // Ticket: User Management - implement the method that allows session information to be
        // stored in it's designated collection.
    	Session session = new Session();
		session.setUserId(userId);
		session.setJwt(jwt);
    	Session currentSession = getUserSession(userId);
    	if(currentSession == null ) {
    		
    		sessionsCollection.insertOne(session);
    	}else {
    		if(currentSession.getJwt() == jwt) {
    			log.warn("Session already exist {}", jwt);
    			return false;
    		}
    		//sessionsCollection.insertOne(session);
    	}
    		

		return true;
    	

        //TODO > Ticket: Handling Errors - implement a safeguard against
        // creating a session with the same jwt token.
    }

    /**
     * Returns the User object matching the an email string value.
     *
     * @param email - email string to be matched.
     * @return User object or null.
     */
    public User getUser(String email) {
        // Ticket: User Management - implement the query that returns the first User object.
        //Bson bson = Aggregates.match(Filters.eq("email", email));
       return  usersCollection.find(new Document("email", email)).first();
    }

    /**
     * Given the userId, returns a Session object.
     *
     * @param userId - user string identifier.
     * @return Session object or null.
     */
    public Session getUserSession(String userId) {
        //Ticket: User Management - implement the method that returns Sessions for a given
        // userId
    	//Bson bson = Aggregates.match(Filters.eq("user_id", userId));
    	return sessionsCollection.find(new Document("user_id", userId)).first();
    }

    public boolean deleteUserSessions(String userId) {
        // Ticket: User Management - implement the delete user sessions method
    	//Bson user = Aggregates.match(Filters.eq("user_id", userId));
    	long deletedDocs = sessionsCollection.deleteOne(new Document("user_id", userId)).getDeletedCount();
    	if(deletedDocs > 0) {
    		return true;
    	}
        return false;
    }

    /**
     * Removes the user document that match the provided email.
     *
     * @param email - of the user to be deleted.
     * @return true if user successfully removed
     */
    public boolean deleteUser(String email) {
        // remove user sessions
        //TODO> Ticket: User Management - implement the delete user method
        //TODO > Ticket: Handling Errors - make this method more robust by
        // handling potential exceptions.
    	//Bson user = Aggregates.match(Filters.eq("email", email));
    	long deletedUsers = usersCollection.deleteOne(new Document("email", email)).getDeletedCount();
    	if(deletedUsers > 0 ) {
    		return true;
    	}
        return false;
    }

    /**
     * Updates the preferences of an user identified by `email` parameter.
     *
     * @param email           - user to be updated email
     * @param userPreferences - set of preferences that should be stored and replace the existing
     *                        ones. Cannot be set to null value
     * @return User object that just been updated.
     */
    public boolean updateUserPreferences(String email, Map<String, ?> userPreferences) {
        //TODO> Ticket: User Preferences - implement the method that allows for user preferences to
        // be updated.
        //TODO > Ticket: Handling Errors - make this method more robust by
        // handling potential exceptions when updating an entry.
    	if(userPreferences == null ) {
    		 throw new IncorrectDaoOperation("user preferences can not be null");
    	}
    	User user = getUser(email);
    	if(user == null ) {
    		throw new IncorrectDaoOperation("user does not exist");
    	}
    	Bson filter = new Document("email",user.getEmail());
    	Bson updateObject  = Updates.set("preferences", userPreferences);
    	try {
    		UpdateResult res = usersCollection.updateOne(filter, updateObject);
        	if(res.getModifiedCount()>0) {
        		return true;
        	}
    	}catch(MongoWriteException e) {
    		log.error("Issue caught while trying to update user");
    		throw new IncorrectDaoOperation(e.getMessage());
    	}
    	
        return false;
    }
}

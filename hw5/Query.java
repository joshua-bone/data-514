/*
* Joshua Bone - UW DATA 514 - HW5 - 2019-03-04
*/

import java.io.FileInputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Runs queries against a back-end database
 */
public class Query
{
  private String configFilename;
  private Properties configProps = new Properties();

  private String jSQLDriver;
  private String jSQLUrl;
  private String jSQLUser;
  private String jSQLPassword;

  // DB Connection
  private Connection conn;

  // Save stateful information
  private Session session = new Session();
  
  // Canned queries
  private static final String CHECK_FLIGHT_CAPACITY = "SELECT capacity FROM Flights WHERE fid = ?";
  private PreparedStatement checkFlightCapacityStatement;
  
  private static final String CHECK_SEATS_BOOKED = "SELECT COUNT(*) AS seats FROM Itineraries " +
		  										   "WHERE flight_id_1 = ? OR flight_id_2 = ?;";
  private PreparedStatement checkSeatsBookedStatement;
  
  private static final String CREATE_USER_SQL = "INSERT INTO Users VALUES (?, ?, ?);";
  private PreparedStatement createUserStatement;
  
  private static final String LOGIN_SQL = "SELECT * FROM Users WHERE username = ? AND password = ?;";
  private PreparedStatement loginStatement;

  private static final String SEARCH_SQL = "" +
		  " DECLARE @origin_city varchar(34) = ?; " +
		  " DECLARE @dest_city varchar(34) = ?; " +
		  " DECLARE @max_flights int = ?; " +
		  " DECLARE @day_of_month int = ?; " +
		  " DECLARE @max_results int = ?; " +

		  " SELECT TOP (@max_results) * FROM ( " +
		  "   SELECT f1.fid AS fid1,  " +
		  "          f2.fid AS fid2,  " +
		  "          f1.actual_time + f2.actual_time AS flight_time, " +
		  "          f1.price + f2.price AS price, " +
		  "          2 AS num_flights " +
		  "       FROM Flights f1 JOIN Flights f2 " +
		  "           ON f1.dest_city = f2.origin_city " +
		  "       WHERE f1.origin_city = @origin_city " +
		  "       AND f2.dest_city = @dest_city " +
		  "       AND f1.day_of_month = @day_of_month " +
		  "       AND f2.day_of_month = @day_of_month " +
		  "       AND f1.canceled != 1 " +
		  "       AND f2.canceled != 1 " +
		  "   UNION ALL " +
		  "   SELECT f.fid AS fid1, " +
		  "          NULL AS fid2, " +
		  "          f.actual_time AS flight_time, " +
		  "          f.price AS price, " +
		  "          1 AS num_flights " +
		  "       FROM Flights f " +
		  "       WHERE f.origin_city = @origin_city " +
		  "       AND f.dest_city = @dest_city " +
		  "       AND f.day_of_month = @day_of_month " +
		  "       AND f.canceled != 1 " +
		  "   ) AS results " +
		  "   WHERE num_flights <= @max_flights " + 
		  "   ORDER BY num_flights, flight_time, fid1, fid2; ";
  private PreparedStatement searchStatement;
  
  private static final String RESERVATIONS_ON_DAY_SQL = "SELECT * FROM Reservations WHERE day_of_month = ? AND username = ?;";
  private PreparedStatement reservationsOnDayStatement;
  
  private static final String CREATE_RESERVATION_SQL = "INSERT INTO Reservations VALUES (?, ?, ?, ?, 0)";
  private PreparedStatement createReservationStatement;
  
  private static final String GET_NEXT_RID_SQL = "SELECT nextRid FROM NextReservationId";
  private PreparedStatement getNextRidStatement;
  
  private static final String SET_NEXT_RID_SQL = "UPDATE NextReservationId SET nextRid = nextRid + 1 WHERE 1=1;";
  private PreparedStatement setNextRidStatement;
  
  private static final String UPDATE_RESERVATION_PAID_SQL = "UPDATE Reservations SET is_paid = 1 WHERE rid = ?;";
  private PreparedStatement updateReservationStatement;
  
  private static final String READ_RESERVATION_SQL = "SELECT * FROM Reservations r " +
		  											 "JOIN Itineraries i ON r.rid = i.reservation_id " +
		  											 "WHERE rid = ? AND username = ?;";
  private PreparedStatement readReservationStatement;
  
  private static final String READ_RESERVATIONS_SQL = "" + 
		  " SELECT  " +
		  " f1.fid AS f1_fid, f2.fid AS f2_fid, f1.year AS f1_year, f2.year AS f2_year, f1.month_id AS f1_month_id, f2.month_id AS f2_month_id,  " +
		  " f1.day_of_month AS f1_day_of_month, f2.day_of_month AS f2_day_of_month, f1.day_of_week_id AS f1_day_of_week_id, f2.day_of_week_id AS f2_day_of_week_id,  " +
		  " f1.carrier_id AS f1_carrier_id, f2.carrier_id AS f2_carrier_id, f1.flight_num AS f1_flight_num, f2.flight_num AS f2_flight_num,  " +
		  " f1.origin_city AS f1_origin_city, f2.origin_city AS f2_origin_city, f1.origin_state AS f1_origin_state, f2.origin_state AS f2_origin_state,  " +
		  " f1.dest_city AS f1_dest_city, f2.dest_city AS f2_dest_city, f1.dest_state AS f1_dest_state, f2.dest_state AS f2_dest_state,  " +
		  " f1.departure_delay AS f1_departure_delay, f2.departure_delay AS f2_departure_delay, f1.taxi_out AS f1_taxi_out, f2.taxi_out AS f2_taxi_out,  " +
		  " f1.arrival_delay AS f1_arrival_delay, f2.arrival_delay AS f2_arrival_delay, f1.canceled AS f1_canceled, f2.canceled AS f2_canceled,  " +
		  " f1.actual_time AS f1_actual_time, f2.actual_time AS f2_actual_time, f1.distance AS f1_distance, f2.distance AS f2_distance,  " +
		  " f1.capacity AS f1_capacity, f2.capacity AS f2_capacity, f1.price AS f1_price, f2.price AS f2_price, * " +
  		  " FROM Reservations r " + 
		  " JOIN Itineraries i ON i.reservation_id = r.rid  " + 
  		  " JOIN Flights f1 ON f1.fid = i.flight_id_1 " + 
  		  " LEFT JOIN Flights f2 ON f2.fid = i.flight_id_2 " + 
  	      " WHERE r.username = ?;";
  private PreparedStatement readReservationsStatement;
  
  private static final String DELETE_RESERVATION_SQL = "DELETE FROM Reservations WHERE rid = ?";
  private PreparedStatement deleteReservationStatement;
  
  private static final String CREATE_ITINERARY_SQL = "INSERT INTO Itineraries VALUES (?, ?, ?)";
  private PreparedStatement createItineraryStatement;
  
  private static final String READ_ITINERARY_SQL = "SELECT * FROM Itineraries WHERE reservation_id = ?";
  private PreparedStatement readItineraryStatement;
  
  private static final String DELETE_ITINERARY_SQL = "DELETE FROM Itineraries WHERE reservation_id = ?;";
  private PreparedStatement deleteItineraryStatement;
  
  private static final String GET_BALANCE_SQL = "SELECT balance FROM Users WHERE username = ?;";
  private PreparedStatement getBalanceStatement;
  
  private static final String SET_BALANCE_SQL = "UPDATE Users SET balance = ? WHERE username = ?;";
  private PreparedStatement setBalanceStatement;
  
  // transactions
  private static final String BEGIN_TRANSACTION_SQL = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;";
  private PreparedStatement beginTransactionStatement;

  private static final String COMMIT_SQL = "COMMIT TRANSACTION";
  private PreparedStatement commitTransactionStatement;

  private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
  private PreparedStatement rollbackTransactionStatement;
  
  // clear tables
  private static final String WIPE_TABLES_SQL = "DELETE FROM Itineraries WHERE 1=1; " + 
		  							            "DELETE FROM Reservations WHERE 1=1; " +
		  										"DELETE FROM Users WHERE 1=1;" +
		  							            "UPDATE NextReservationId SET nextRid = 1 WHERE 1=1;";
  private PreparedStatement wipeTablesStatement;

  public Query(String configFilename)
  {
    this.configFilename = configFilename;
  }

  /* Connection code to SQL Azure.  */
  public void openConnection() throws Exception
  {
    configProps.load(new FileInputStream(configFilename));

    jSQLDriver = configProps.getProperty("flightservice.jdbc_driver");
    jSQLUrl = configProps.getProperty("flightservice.url");
    jSQLUser = configProps.getProperty("flightservice.sqlazure_username");
    jSQLPassword = configProps.getProperty("flightservice.sqlazure_password");

		/* load jdbc drivers */
    Class.forName(jSQLDriver).newInstance();

		/* open connections to the flights database */
    conn = DriverManager.getConnection(jSQLUrl, // database
            jSQLUser, // user
            jSQLPassword); // password

    conn.setAutoCommit(true); //by default automatically commit after each statement

		/* You will also want to appropriately set the transaction's isolation level through:
		   conn.setTransactionIsolation(...)
		   See Connection class' JavaDoc for details.
		 */
  }

  public void closeConnection() throws Exception
  {
    conn.close();
  }

  /**
   * Clear the data in any custom tables created. Do not drop any tables and do not
   * clear the flights table. You should clear any tables you use to store reservations
   * and reset the next reservation ID to be 1.
   */
  public void clearTables ()
  {
	executeUpdate(wipeTablesStatement);
  }

	/**
   * prepare all the SQL statements in this method.
   * "preparing" a statement is almost like compiling it.
   * Note that the parameters (with ?) are still not filled in
   */
  public void prepareStatements() throws Exception
  {
    beginTransactionStatement = conn.prepareStatement(BEGIN_TRANSACTION_SQL);
    commitTransactionStatement = conn.prepareStatement(COMMIT_SQL);
    rollbackTransactionStatement = conn.prepareStatement(ROLLBACK_SQL);

    checkFlightCapacityStatement = conn.prepareStatement(CHECK_FLIGHT_CAPACITY);
    createUserStatement = conn.prepareStatement(CREATE_USER_SQL);
    loginStatement = conn.prepareStatement(LOGIN_SQL);
    searchStatement = conn.prepareStatement(SEARCH_SQL);
    reservationsOnDayStatement = conn.prepareStatement(RESERVATIONS_ON_DAY_SQL);
    createReservationStatement = conn.prepareStatement(CREATE_RESERVATION_SQL);
    getNextRidStatement = conn.prepareStatement(GET_NEXT_RID_SQL);
    setNextRidStatement = conn.prepareStatement(SET_NEXT_RID_SQL);
    createItineraryStatement = conn.prepareStatement(CREATE_ITINERARY_SQL);
    readItineraryStatement = conn.prepareStatement(READ_ITINERARY_SQL);
    deleteItineraryStatement = conn.prepareStatement(DELETE_ITINERARY_SQL);
    setBalanceStatement = conn.prepareStatement(SET_BALANCE_SQL);
    getBalanceStatement = conn.prepareStatement(GET_BALANCE_SQL);
    readReservationStatement = conn.prepareStatement(READ_RESERVATION_SQL);
    readReservationsStatement = conn.prepareStatement(READ_RESERVATIONS_SQL);
    deleteReservationStatement = conn.prepareStatement(DELETE_RESERVATION_SQL);
    updateReservationStatement = conn.prepareStatement(UPDATE_RESERVATION_PAID_SQL);
    wipeTablesStatement = conn.prepareStatement(WIPE_TABLES_SQL);
    checkSeatsBookedStatement = conn.prepareStatement(CHECK_SEATS_BOOKED);
  }

  /**
   * Takes a user's username and password and attempts to log the user in.
   *
   * @param username
   * @param password
   *
   * @return If someone has already logged in, then return "User already logged in\n"
   * For all other errors, return "Login failed\n".
   *
   * Otherwise, return "Logged in as [username]\n".
   */
  public String transaction_login(String username, String password)
  {
	  try {
		  if (username.equals(session.username)) {
			  return username + " is already logged in.";
		  }
	      Optional<ResultSet> result = executeQuery(loginStatement, 
	    		  									Param.ofString(username),
	    		  									Param.ofString(password));
	      boolean success = result.isPresent() && result.get().next();
	      if (success) {
	    	  // Clear state only if previously logged in as another username
	    	  if (session.username != null) {
	    		  session = new Session();
	    	  }
	    	  session.username = username;
	      }
	      return success ? "Logged in as " + username + "\n" : "Login failed\n";
	  } catch (SQLException e) {
		  throw new RuntimeException(e);
	  }
  }

  /**
   * Implement the create user function.
   *
   * @param username new user's username. User names are unique the system.
   * @param password new user's password.
   * @param initAmount initial amount to deposit into the user's account, should be >= 0 (failure otherwise).
   *
   * @return either "Created user {@code username}\n" or "Failed to create user\n" if failed.
   */
  public String transaction_createCustomer (String username, String password, double initAmount)
  {
	  boolean success = initAmount >= 0;
	  success = success & executeUpdate(createUserStatement, 
			  						    Param.ofString(username), 
			  						    Param.ofString(password), 
			  						    Param.ofDouble(initAmount));
	  return success ? "Created user " + username + "\n" : "Failed to create user\n";
  }

  /**
   * Implement the search function.
   *
   * Searches for flights from the given origin city to the given destination
   * city, on the given day of the month. If {@code directFlight} is true, it only
   * searches for direct flights, otherwise is searches for direct flights
   * and flights with two "hops." Only searches for up to the number of
   * itineraries given by {@code numberOfItineraries}.
   *
   * The results are sorted based on total flight time.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight if true, then only search for direct flights, otherwise include indirect flights as well
   * @param dayOfMonth
   * @param numberOfItineraries number of itineraries to return
   *
   * @return If no itineraries were found, return "No flights match your selection\n".
   * If an error occurs, then return "Failed to search\n".
   *
   * Otherwise, the sorted itineraries printed in the following format:
   *
   * Itinerary [itinerary number]: [number of flights] flight(s), [total flight time] minutes\n
   * [first flight in itinerary]\n
   * ...
   * [last flight in itinerary]\n
   *
   * Each flight should be printed using the same format as in the {@code Flight} class. Itinerary numbers
   * in each search should always start from 0 and increase by 1.
   *
   * @see Flight#toString()
   */
  public String transaction_search(String originCity, String destinationCity, boolean directFlight, int dayOfMonth,
                                   int numberOfItineraries)
  {
	  session.reset();
	  int maxHops = directFlight ? 1 : 2;
	  try {
		  Optional<ResultSet> optResult = executeQuery(searchStatement,
				  									Param.ofString(originCity),
				  									Param.ofString(destinationCity),
				  									Param.ofInt(maxHops),
				  									Param.ofInt(dayOfMonth),
				  									Param.ofInt(numberOfItineraries));
		  if (!optResult.isPresent()) {
			  return "Failed to search\n";
		  }
		  ResultSet result = optResult.get();
		  while (result.next()) {
			  session.inMemoryItineraries.add(Itinerary.read(result));
		  }
		  if (session.inMemoryItineraries.isEmpty()) {
			  return "No flights match your selection\n";
		  }
		  session.dayOfMonth = dayOfMonth;
		  loadFlightInfo();
		  StringBuilder sb = new StringBuilder();
		  for (int i = 0; i < session.inMemoryItineraries.size(); i++) {
			  Itinerary it = session.inMemoryItineraries.get(i);
			  sb.append(String.format("Itinerary %d: %s\n", i, it.summary()));
			  for (int fid : it.flightIds) {
				  Flight f = session.inMemoryFlights.get(fid);
				  sb.append(f.toString());
				  sb.append("\n");
			  }
		  }
		  return sb.toString();
	  } catch (SQLException e) {
		  throw new RuntimeException(e);
	  }	  
  }

  /**
   * Implements the book itinerary function.
   *
   * @param itineraryId ID of the itinerary to book. This must be one that is returned by search in the current session.
   *
   * @return If the user is not logged in, then return "Cannot book reservations, not logged in\n".
   * If try to book an itinerary with invalid ID, then return "No such itinerary {@code itineraryId}\n".
   * If the user already has a reservation on the same day as the one that they are trying to book now, then return
   * "You cannot book two flights in the same day\n".
   * For all other errors, return "Booking failed\n".
   *
   * And if booking succeeded, return "Booked flight(s), reservation ID: [reservationId]\n" where
   * reservationId is a unique number in the reservation system that starts from 1 and increments by 1 each time a
   * successful reservation is made by any user in the system.
   */
  public String transaction_book(int itineraryId)
  {
	  try {
		  if (session.username == null) {
			  return "Cannot book reservations, not logged in\n";
		  } else if (itineraryId < 0 || itineraryId >= session.inMemoryItineraries.size()) {
			  return String.format("No such itinerary %d", itineraryId);
		  }
		  Optional<ResultSet> optResult = executeQuery(reservationsOnDayStatement, 
				  									   Param.ofInt(session.dayOfMonth), 
				  									   Param.ofString(session.username));
		  if (optResult.isPresent() && optResult.get().next()) {
			  return "You cannot book two flights in the same day\n";
		  }
		  beginTransaction();
		  Itinerary it = session.inMemoryItineraries.get(itineraryId);
		  if (!createReservation(it) || isOverbooked(it.flightIds.get(0)) || 
				  (it.flightIds.size() == 2 && isOverbooked(it.flightIds.get(1)))) {
			  rollbackTransaction();
			  session.resetReservation();
			  return "Booking failed\n";
		  } else {
			  commitTransaction();
			  return String.format("Booked flight(s), reservation ID: %d\n", session.lastReservationId);
		  }
	  } catch (SQLException e) {
		  throw new RuntimeException(e);
	  }
  }

  /**
   * Implements the reservations function.
   *
   * @return If no user has logged in, then return "Cannot view reservations, not logged in\n"
   * If the user has no reservations, then return "No reservations found\n"
   * For all other errors, return "Failed to retrieve reservations\n"
   *
   * Otherwise return the reservations in the following format:
   *
   * Reservation [reservation ID] paid: [true or false]:\n"
   * [flight 1 under the reservation]
   * [flight 2 under the reservation]
   * Reservation [reservation ID] paid: [true or false]:\n"
   * [flight 1 under the reservation]
   * [flight 2 under the reservation]
   * ...
   *
   * Each flight should be printed using the same format as in the {@code Flight} class.
   *
   * @see Flight#toString()
   */
  public String transaction_reservations()
  {
	 if (!session.isLoggedIn()) return "Cannot view reservations, not logged in\n";
	 String failString = "Failed to retrieve reservations\n";
	 try {
		 Optional<ResultSet> optResult = executeQuery(readReservationsStatement, Param.ofString(session.username));
		 if (!optResult.isPresent()) return failString;
		 ResultSet result = optResult.get();
		 if (!result.next()) return "No reservations found\n";
		 
		 StringBuilder sb = new StringBuilder();
		 do {
			 sb.append(String.format("Reservation %d paid: %s:\n", 
	 			     result.getInt("rid"), 
	 			     result.getInt("is_paid") == 1));
			 sb.append(Flight.read(result, "f1_").toString());
			 sb.append("\n");
			 result.getInt("flight_id_2");
			 if (!result.wasNull()) {
				 sb.append(Flight.read(result, "f2_").toString());
				 sb.append("\n");
			 }
		 } while (result.next());
		 return sb.toString();
	 } catch (SQLException e) {
		 throw new RuntimeException(e);
	 }
  }

  /**
   * Implements the cancel operation.
   *
   * @param reservationId the reservation ID to cancel
   *
   * @return If no user has logged in, then return "Cannot cancel reservations, not logged in\n"
   * For all other errors, return "Failed to cancel reservation [reservationId]"
   *
   * If successful, return "Canceled reservation [reservationId]"
   *
   * Even though a reservation has been canceled, its ID should not be reused by the system.
   */
  public String transaction_cancel(int reservationId)
  {
	  if (!session.isLoggedIn()) return "Cannot cancel reservations, not logged in\n";
	  try {
		  Optional<ResultSet> optResult = null;
		  ResultSet result = null;

		  boolean txnFailed = false;
		  
		  beginTransaction();
		  
		  if (!txnFailed) optResult = executeQuery(readReservationStatement, 
				  									   Param.ofInt(reservationId), 
				  									   Param.ofString(session.username));
		  txnFailed = txnFailed || !optResult.isPresent(); //fail if query failed
		  if (!txnFailed) result = optResult.get();
		  txnFailed = txnFailed || !result.next(); //fail if reservation not found
		  txnFailed = txnFailed || result.getInt("is_paid") == 1 && !refundAmt(result.getDouble("price")); //fail if refund fails
		  txnFailed = txnFailed || !executeUpdate(deleteItineraryStatement, Param.ofInt(reservationId)); //fail if can't delete itinerary
		  txnFailed = txnFailed || !executeUpdate(deleteReservationStatement, Param.ofInt(reservationId)); //fail if can't delete reservation
		  
		  if (txnFailed) {
			  rollbackTransaction();
			  return String.format("Failed to cancel reservation %d\n", reservationId);
		  } else {
			  commitTransaction();
			  return String.format("Canceled reservation %d\n", reservationId);
		  }
		  
	  } catch (SQLException e) {
		  throw new RuntimeException(e);
	  }
  }

  /**
   * Implements the pay function.
   *
   * @param reservationId the reservation to pay for.
   *
   * @return If no user has logged in, then return "Cannot pay, not logged in\n"
   * If the reservation is not found / not under the logged in user's name, then return
   * "Cannot find unpaid reservation [reservationId] under user: [username]\n"
   * If the user does not have enough money in their account, then return
   * "User has only [balance] in account but itinerary costs [cost]\n"
   * For all other errors, return "Failed to pay for reservation [reservationId]\n"
   *
   * If successful, return "Paid reservation: [reservationId] remaining balance: [balance]\n"
   * where [balance] is the remaining balance in the user's account.
   */
  public String transaction_pay (int reservationId)
  {
	  if (!session.isLoggedIn()) return "Cannot cancel reservations, not logged in\n";
	  try {
		  Optional<ResultSet> optResult = null;
		  ResultSet result = null;
		  Double balance = null;
		  Double cost = null;
		  boolean isEmpty = false;
		  
		  boolean txnFailed = false;
		  beginTransaction();
		  optResult = executeQuery(readReservationStatement, 
				  				   Param.ofInt(reservationId),
				  				   Param.ofString(session.username));
		  txnFailed = txnFailed || !optResult.isPresent(); //fail if query failed
		  if(!txnFailed) {
			  result = optResult.get();
			  isEmpty = !result.next();
			  txnFailed = txnFailed || isEmpty;
		  }
		  txnFailed = txnFailed || result.getInt("is_paid") == 1; //fail if already paid
		  if (!txnFailed) {
			  balance = getBalance();
			  cost = result.getDouble("price");
		  }
		  txnFailed = txnFailed || !chargeAmt(cost); //fail if can't charge user
		  txnFailed = txnFailed || !markPaid(reservationId); //fail if can't mark paid
		  
		  if (txnFailed) {
			  rollbackTransaction();
			  boolean wereNoUnpaidResults = isEmpty || result.getInt("is_paid") == 1;
			  boolean wasEmptyBalance = cost != null && balance != null && cost > balance;
			  if (wereNoUnpaidResults) {
				  return String.format("Cannot find unpaid reservation under user: %s\n", session.username);
			  } else if (wasEmptyBalance) {
				  return String.format("User has only %.2f in account but itinerary costs %.2f\n", balance, cost);
			  } else {
				  return String.format("Failed to pay for reservation %d\n", reservationId);
			  }
		  } else {
			  commitTransaction();
			  return String.format("Paid reservation: %d remaining balance: %.2f\n", reservationId, getBalance());
		  }
	  } catch (SQLException e) {
		  throw new RuntimeException(e);
	  }
  }

  /* some utility functions below */

  public void beginTransaction() throws SQLException
  {
    conn.setAutoCommit(false);
    beginTransactionStatement.executeUpdate();
  }

  public void commitTransaction() throws SQLException
  {
    commitTransactionStatement.executeUpdate();
    conn.setAutoCommit(true);
  }

  public void rollbackTransaction() throws SQLException
  {
    rollbackTransactionStatement.executeUpdate();
    conn.setAutoCommit(true);
  }
  
  private void loadFlightInfo() throws SQLException {
	  String fids = session.inMemoryItineraries.stream()
			  										  .flatMap(it -> it.flightIds.stream())
			  										  .map(fid -> fid.toString())
			  										  .collect(Collectors.joining(","));
	  PreparedStatement stmt = conn.prepareStatement((String.format("SELECT * FROM Flights WHERE fid IN (%s)", fids)));
	  ResultSet results = executeQuery(stmt).get();
	  while (results.next()) {
		  Flight f = Flight.read(results);
		  session.inMemoryFlights.put(f.fid, f);
	  }
  }
  
  private boolean executeUpdate(PreparedStatement stmt, Param... paramPairs){
	  try {
		  bindParams(stmt, paramPairs);
		  return stmt.executeUpdate() > 0;
	  } catch (SQLException e) {
		  return false;
	  }
  }
  
  private Optional<ResultSet> executeQuery(PreparedStatement stmt, Param... paramPairs) {
	  try {
		  bindParams(stmt, paramPairs);
		  return Optional.of(stmt.executeQuery());
	  } catch (SQLException e) {
		  return Optional.empty();
	  }
  }
	  
  private void bindParams(PreparedStatement stmt, Param... paramPairs) throws SQLException {
	  for (int i = 0; i < paramPairs.length; i++) {
		  Param pair = paramPairs[i];
		  pair.lambda.accept(stmt, i + 1, pair.field); // fields are indexed starting from 1, not 0
	  }
  }
  
  private boolean createReservation(Itinerary it) throws SQLException {
	  int rid = nextRidPlusPlus();
	  session.lastReservationId = rid;
	  boolean success = executeUpdate(createReservationStatement,
							  		  Param.ofInt(rid),
							  		  Param.ofString(session.username),
							  		  Param.ofInt(session.dayOfMonth),
							  		  Param.ofDouble(it.price));

	  Param[] params = new Param[1 + it.flightIds.size()];
	  params[0] = Param.ofInt(rid);
	  params[1] = Param.ofInt(it.flightIds.get(0));
	  if (params.length == 3) {
		  params[2] = Param.ofInt(it.flightIds.get(1));
	  }
	  return success && executeUpdate(createItineraryStatement, 
			  			              Param.ofInt(rid),
			  			              Param.ofInt(it.flightIds.get(0)),
			  			              it.flightIds.size() == 2 ? 
			  			            		  Param.ofInt(it.flightIds.get(1)) : 
			  			            		  Param.ofNullInt());
  }
  
  private int nextRidPlusPlus() throws SQLException {
	  ResultSet result = executeQuery(getNextRidStatement).get();
	  result.next();
	  executeUpdate(setNextRidStatement);
	  return result.getInt("nextRid");
  }
  
  private boolean isOverbooked(int fid) throws SQLException {
	  return getCapacity(fid) < getBookedSeats(fid);
  }
  
  private int getCapacity(int fid) throws SQLException {
	  ResultSet result = executeQuery(checkFlightCapacityStatement, Param.ofInt(fid)).get();
	  result.next();
	  return result.getInt("capacity");
  }
  
  private int getBookedSeats(int fid) throws SQLException {
	  ResultSet result = executeQuery(checkSeatsBookedStatement, Param.ofInt(fid), Param.ofInt(fid)).get();
	  result.next();
	  return result.getInt("seats");
  }
  
  private double getBalance() throws SQLException {
	  ResultSet result = executeQuery(getBalanceStatement,
			  						  Param.ofString(session.username)).get();
	  result.next();
	  return result.getDouble("balance");
  }
  
  private boolean markPaid(int rid) throws SQLException {
	  return executeUpdate(updateReservationStatement, Param.ofInt(rid));
  }
  
  private boolean chargeAmt(double amt) throws SQLException {
	  double current = getBalance();
	  if (amt > 0 && current > amt) {
		  return executeUpdate(setBalanceStatement,
				  		       Param.ofDouble(current - amt),
				  		       Param.ofString(session.username));
	  }
	  return false;
  }
  
  private boolean refundAmt(double amt) throws SQLException {
	  double current = getBalance();
	  if (amt > 0) {
		  return executeUpdate(setBalanceStatement,
				  		       Param.ofDouble(current + amt),
				  		       Param.ofString(session.username));
	  }
	  return false;
  }
  
  @FunctionalInterface
  interface SQLTriConsumer<A,B,C> {
      void accept(A a, B b, C c) throws SQLException;
  }
  
  private static class Param {
	  SQLTriConsumer<PreparedStatement, Integer, Object> lambda;
	  Object field;
	  // Canned lambdas to save code repetition
	  private static final SQLTriConsumer<PreparedStatement, Integer, Object> bindString = 
			  ((stmt, i, o) -> stmt.setString(i, (String) o));
	  private static final SQLTriConsumer<PreparedStatement, Integer, Object> bindDouble = 
			  ((stmt, i, o) -> stmt.setDouble(i, (Double) o));
	  private static final SQLTriConsumer<PreparedStatement, Integer, Object> bindInt = 
			  ((stmt, i, o) -> stmt.setInt(i, (Integer) o));
	  private static final SQLTriConsumer<PreparedStatement, Integer, Object> bindNullInt = 
			  ((stmt, i, o) -> stmt.setNull(i, java.sql.Types.INTEGER));
	  
	  static Param ofString(String s) {
		  return of(bindString, (Object) s);
	  }
	  
	  static Param ofDouble(Double d) {
		  return of(bindDouble, (Object) d);
	  }
	  
	  static Param ofInt(Integer i) {
		  return of(bindInt, (Object) i);
	  }
	  
	  static Param ofNullInt() {
		  return of(bindNullInt, null);
	  }
	  
	  private static Param of(SQLTriConsumer<PreparedStatement, Integer, Object> lambda, Object field) {
		  Param pair = new Param();
		  pair.lambda = lambda;
		  pair.field = field;
		  return pair;
	  }
  }
  
  private static class Flight
  {
	private static final String FORMAT_STRING = "ID: %d Date: %4d-%d-%d Carrier: %s Number: %s Origin: %s " +
												"Dest: %s Duration: %.1f Capacity: %d Price: %.2f";
    public int fid;
    public int year;
    public int monthId;
    public int dayOfMonth;
    public String carrierId;
    public String flightNum;
    public String originCity;
    public String destCity;
    public double time;
    public int capacity;
    public double price;

    @Override
    public String toString()
    {
    	return String.format(FORMAT_STRING, fid, year, monthId, dayOfMonth, carrierId, flightNum, originCity,
    						 destCity, time, capacity, price);
    }
    
    static Flight read(ResultSet set) {
    	return read(set, "");
    }
    
    static Flight read(ResultSet set, String pre) {
    	try {
    		Flight f = new Flight();
    		f.fid = set.getInt(pre + "fid");
    		f.year = set.getInt(pre + "year");
    		f.monthId = set.getInt(pre + "month_id");
    		f.dayOfMonth = set.getInt(pre + "day_of_month");
    		f.carrierId = set.getString(pre + "carrier_id");
    		f.flightNum = Integer.toString(set.getInt(pre + "flight_num"));
    		f.originCity = set.getString(pre + "origin_city");
    		f.destCity = set.getString(pre + "dest_city");
    		f.time = set.getDouble(pre + "actual_time");
    		f.capacity = set.getInt(pre + "capacity");
    		f.price = set.getDouble(pre + "price");
    		return f;
    	} catch (SQLException e) {
    		throw new RuntimeException(e);
    	}
    }
  }
  
  private static class Itinerary {
	  List<Integer> flightIds = new ArrayList<>();
	  float flight_time;
	  double price;
	  
	  public String summary() {
		  return String.format("%d flight(s), %.1f minutes", flightIds.size(), flight_time);
	  }
	  
	  static Itinerary read(ResultSet set) {
		  try {
			  Itinerary i = new Itinerary();
			  i.flightIds.add(set.getInt("fid1"));
			  int flightId2 = set.getInt("fid2");
			  if (!set.wasNull()) {
				  i.flightIds.add(flightId2);
			  }
			  i.flight_time = set.getFloat("flight_time");
			  i.price = set.getDouble("price");
			  return i;
		  } catch (SQLException e) {
			  throw new RuntimeException(e);
		  }
	  }
  }
  
  private static class Session {
	  String username; //currently logged in username, or null if logged out
	  
	  //store info from recent actions
	  int dayOfMonth = -1;
	  int lastReservationId = -1;
	  Map<Integer, Flight> inMemoryFlights = new HashMap<>();
	  List<Itinerary> inMemoryItineraries = new ArrayList<>();
	  
	  boolean isLoggedIn() {
		  return username != null;
	  }
	  
	  void reset() {
		  dayOfMonth = -1;
		  lastReservationId = -1;
		  inMemoryFlights.clear();
		  inMemoryItineraries.clear();
	  }
	  
	  void resetReservation() {
		  lastReservationId = -1;
	  }
  }
}
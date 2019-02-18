#!/usr/bin/env python3

from modules import pg8000
import configparser
import json


#####################################################
##  Database Connect
#####################################################

'''
Connects to the database using the connection string
'''
def database_connect():
    # Read the config file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Create a connection to the database
    connection = None
    try:
        # Parses the config file and connects using the connect string
        connection = pg8000.connect(database=config['DATABASE']['user'],
                                    user=config['DATABASE']['user'],
                                    password=config['DATABASE']['password'],
                                    host=config['DATABASE']['host'])
    except pg8000.OperationalError as e:
        print("""Error, you haven't updated your config.ini or you have a bad
        connection, please try again. (Update your files first, then check
        internet connection)
        """)
        print(e)
    # return the connection to use
    return connection

#####################################################
##  Login
#####################################################

'''
Check that the users information exists in the database.

- True = return the user data
- False = return None
'''
def check_login(member_id, password):

    con = database_connect()
    if (con is None):
        return None
    cur = con.cursor()
    try:
        # Try executing the SQL and get from the database
        sql = """SELECT M.member_id, M.title, M.given_names, M.family_name, C.country_name, P.place_name
                 FROM member M INNER JOIN Country C USING (country_code) JOIN Place P ON (M.accommodation = P.place_id)
                 WHERE member_id = %s AND pass_word=%s;"""
        cur.execute(sql, (member_id, password))
        user_data = cur.fetchone()  #Fetch the first rows

        #ASK WHERE TO PUT COMMITS

        member_id_test = [user_data[0]]
        sql = """SELECT member_id
                 FROM Athlete
                 where member_id = %s;"""
        cur.execute(sql, member_id_test)

        user_type = None
        if (cur.rowcount == 1):
            user_type = 'Athlete'
        else:
            sql = """SELECT member_id
                     FROM Official
                     WHERE member_id = %s;"""
            cur.execute(sql, member_id_test)
            if (cur.rowcount ==1) :
                user_type = 'Official'
            else:
                user_type = 'Staff'

    except:
        con.rollback()
        return None
    cur.close()
    con.close()

    tuples = {
            'member_id': user_data[0],
            'title': user_data[1],
            'first_name': user_data[2],
            'family_name': user_data[3],
            'country_name': user_data[4],
            'residence': user_data[5],
            'member_type': user_type
        }

    return tuples


#####################################################
## Member Information
#####################################################

'''
Get the details for a member, including:
    - all the accommodation details,
    - information about their events
    - medals
    - bookings.

If they are an official, then they will have no medals, just a list of their roles.
'''
def member_details(member_id, mem_type):

    con = database_connect()
    if (con is None):
        return None
    cur = con.cursor()

    try:
        sql =  """SELECT P.place_name, P.address, P.gps_lat, P.gps_long
                FROM Place P
                WHERE P.place_id = (SELECT accommodation
                                    FROM Member
                                    WHERE member_id=%s);"""
        cur.execute(sql, [member_id])
        user_data = cur.fetchone()
        #print(user_data)

    except:
        con.rollback()
        return None


    accommodation_details = {
            'name': user_data[0],
            'address': user_data[1],
            'gps_lat': user_data[2],
            'gps_long': user_data[3]
        }

    # TODO
    # Return all of the user details including subclass-specific details
    #   e.g. events participated, results.

    # TODO - Dummy Data (Try to keep the same format)
    # Accommodation [name, address, gps_lat, gps_long]
    # accom_rows = ['SIT', '123 Some Street, Boulevard', '-33.887946', '151.192958']

    # Check what type of member we are
    if(mem_type == 'athlete'):
        # TODO get the details for athletes
        # Member details [total events, total gold, total silver, total bronze, number of bookings]

        # Counting total events
        try:
            sql = """SELECT ((SELECT COUNT(event_id)
                            FROM Participates
                            WHERE athlete_id = %s)
                            +
                            (SELECT COUNT(event_id)
                            FROM TeamMember
                            WHERE athlete_id = %s))"""
            cur.execute(sql, [member_id])
            event_count = cur.fetchone()

                            # Counting medals
            sql = """SELECT ((SELECT COUNT(medal)
                            FROM Participates
                            WHERE athlete_id = %s AND medal = 'G')
                            +
                            (SELECT COUNT(medal)
                            FROM Team JOIN TeamMember USING (event_id, team_name)
                            WHERE athlete_id = %s AND medal = 'G')) as num_gold,
                            ((SELECT COUNT(medal)
                            FROM Participates
                            WHERE athlete_id = %s AND medal = 'S')
                            +
                            (SELECT COUNT(medal)
                            FROM Team JOIN TeamMember USING (event_id, team_name)
                            WHERE athlete_id = %s AND medal = 'S')) as num_silver,
                            ((SELECT COUNT(medal)
                            FROM Participates
                            WHERE athlete_id = %s AND medal = 'B')
                            +
                            (SELECT COUNT(medal)
                            FROM Team JOIN TeamMember USING (event_id, team_name)
                            WHERE athlete_id = %s AND medal = 'B')) as num_bronze"""
            cur.execute(sql, [member_id])
            medals_db = cur.fetchone()

            # Counting number of bookings
            sql = """SELECT COUNT(*)
                     FROM Booking
                     WHERE booked_for = %s;"""
            cur.execute(sql, [member_id])
            booking_count = cur.fetchone()

        except:
            con.rollback()
            return None

        member_information = {
            'total_events': event_count[0],
            'gold': medals_db[0],
            'silver': medals_db[1],
            'bronze': medals_db[2],
            'bookings': booking_count[0]
        }

    elif(mem_type == 'official'):

        # TODO get the relevant information for an official
        # Official = [ Role with greatest count, total event count, number of bookings]
        # member_information_db = ['Judge', 10, 20]
        try:
            sql = """SELECT role, COUNT(role)
                    FROM RunsEvent
                    WHERE member_id = %s
                    GROUP BY role
                    ORDER BY COUNT(role) DESC
                    LIMIT 1"""
            cur.execute(sql, [member_id])
            role_info = cur.fetchone()

            sql = """SELECT (SELECT COUNT(event_id)
                            FROM RunsEvent
                            WHERE member_id = %s) AS event_count,
                            (SELECT COUNT(*)
                            FROM Booking
                            WHERE booked_for = %s) AS num_bookings"""
            cur.execute(sql, [member_id])
            official_info = cur.fetchone()
        except:
            con.rollback()
            return None

        member_information = {
            'favourite_role' : role_info[0],
            'total_events' : official_info[0],
            'bookings': official_info[1]
        }
    else:
        try:
            sql = """SELECT COUNT(*)
                    FROM Booking
                    WHERE booked_by = %s;"""
            cur.execute(sql, [member_id])
            booked_by_count = cur.fetchone()
        except:
            con.rollback()
            return None


        # TODO get information for staff member
        # Staff = [number of bookings ]
        #member_information_db = [10]
        member_information = {'bookings': booked_by_count[0]}


    cur.close()
    con.close()
    #return user_data
    # Leave the return, this is being handled for marking/frontend.
    return {'accommodation': accommodation_details, 'member_details': member_information}

#####################################################
##  Booking (make, get all, get details)
#####################################################

'''
Make a booking for a member.
Only a staff type member should be able to do this ;)
Note: `my_member_id` = Staff ID (bookedby)
      `for_member` = member id that you are booking for
'''
def make_booking(my_member_id, for_member, vehicle, date, hour, start_destination, end_destination):

    con = database_connect()
    if (con is None):
        return None

    cur = con.cursor()

    try:

        con.tpc_begin('booking')

        sql = """SELECT member_id
                 FROM Staff
                 WHERE member_id = %s;"""
        cur.execute(sql, [my_member_id])

        if (cur.rowcount != 1):
            return False

        sql = """SELECT capacity, nbooked, journey_id
                 FROM Vehicle JOIN Journey USING (vehicle_code)
                 WHERE vehicle_code = %s
                       AND to_place = %s
                       AND from_place = %s
                       AND depart_time = '%s' || '%s';"""
        cur.execute(sql, (vehicle, start_destination, end_destination, date, hour))
        info = cur.fetchone()

        if (info is None):
            con.tpc_rollback('booking')
            return False
        elif (info[0] < info[1] + 1):
            con.tpc_rollback('booking')
            return False

        con.tpc_prepare()

        sql = """INSERT INTO Booking VALUES (%s, %s, CURRENT_TIMESTAMP, %s);"""
        cur.execute(sql, (for_member, my_member_id, info[2]))

        sql = """UPDATE Journey SET nbooked = nbooked + 1 WHERE journey_id = %s;"""
        cur.execute(sql, [info[2]])

        con.tpc_commit('booking')

    except:
        con.rollback()
        return False

    cur.close()
    con.close()


    # TODO - make a booking
    # Insert a new booking
    # Only a staff member should be able to do this!!
    # Make sure to check for:
    #       - If booking > capacity
    #       - Check the booking exists for that time/place from/to.
    #       - Update nbooked
    #       - Etc.
    # return False if booking was unsuccessful :)
    # We want to make sure we check this thoroughly
    # MUST BE A TRANSACTION ;)
    return True

'''
List all the bookings for a member
'''
def all_bookings(member_id):

    con = database_connect()
    if (con is None):
        return None
    cur = con.cursor()

    try:
        sql = """SELECT vehicle_code, date(depart_time), "time"(depart_time), to_place, from_place
        FROM Journey JOIN Booking USING (journey_id)
        WHERE booked_for = %s;"""
        cur.execute(sql, [member_id])
        bookings_db = cur.fetchall()
    except:
        con.rollback()
        return None
    cur.close()
    con.close()

            # TODO - fix up the bookings_db information
            # Get all the bookings for this member's ID
            # You might need to join a few things ;)
            # It will be a list of lists - e.g. your rows

            # Format:
            # [
            #    [ vehicle, startday, starttime, to, from ],
            #   ...
            # ]

    bookings = [{
        'vehicle': row[0],
        'start_day': row[1],
        'start_time': row[2],
        'to': row[3],
        'from': row[4]
    } for row in bookings_db]

    return bookings

'''
List all the bookings for a member on a certain day
'''
def day_bookings(member_id, day):

    con = database_connect()
    if (con is None):
        return None

    cur = con.cursor()

    try:

        sql = """SELECT vehicle_code, "time"(depart_time), date(depart_time), to_place, from_place
                 FROM Journey JOIN Booking USING (journey_id)
                 WHERE startday = %s
                       AND booked_for = %s;"""
        cur.execute(sql, (day, member_id))
        bookings_db = cur.fetchall()

    except:
        con.rollback()
        return False

    cur.close()
    con.close()

    # TODO - fix up the bookings_db information
    # Get bookings for the member id for just one day
    # You might need to join a few things ;)
    # It will be a list of lists - e.g. your rows

    # Format:
    # [
    #    [ vehicle, startday, starttime, to, from ],
    #   ...
    # ]

    bookings = [{
        'vehicle': row[0],
        'start_day': row[1],
        'start_time': row[2],
        'to': row[3],
        'from': row[4]
    } for row in bookings_db]

    return bookings


'''
Get the booking information for a specific booking
'''
def get_booking(b_date, b_hour, vehicle, from_place, to_place, member_id):

    # TODO - fix up the row to get booking information
    # Get the information about a certain booking, including who booked etc.
    # It will include more detailed information

    # Format:
    #   [vehicle, startday, starttime, to, from, booked_by (name of person), when booked]
    # row = ['TR870R', '21/12/2020', '0600', 'SIT', 'Wentworth', 'Mike', '21/12/2012']

    con = database_connect()
    if (con is None):
        return None
    cur = con.cursor()

    try:
        sql = """SELECT vehicle_code, date(depart_time), "time"(depart_time), to_place, from_place, M.given_names, date(when_booked)
                 FROM Journey JOIN Booking USING (journey_id)
                              JOIN Member M ON (booked_by=M.member_id)
		                      JOIN Member A ON (booked_for=A.member_id)
                 WHERE date(depart_time) = %s AND "time"(depart_time) = %s AND vehicle_code = %s AND from_place = %s AND to_place = %s AND A.member_id = %s;"""

        cur.execute(sql, (b_date, b_hour, vehicle, from_place, to_place, member_id))
        booking_db = cur.fetchone()

    except:
        con.rollback()
        return None
    cur.close()
    con.close()

    booking = {
        'vehicle': booking_db[0],
        'start_day': booking_db[1],
        'start_time': booking_db[2],
        'to': booking_db[3],
        'from': booking_db[4],
        'booked_by': booking_db[5],
        'whenbooked': booking_db[6]
    }

    return booking

#####################################################
## Journeys
#####################################################

'''
List all the journeys between two places.
'''
def all_journeys(from_place, to_place):

    con = database_connect()
    if (con is None):
        return None

    cur = con.cursor()

    try:
        sql = """WITH RECURSIVE Connection AS (
                    SELECT vehicle_code, date(depart_time), "time"(depart_time), to_place, from_place, nbooked, capacity
                    FROM Journey JOIN Vehicle USING (vehicle_code)
                    WHERE from_place = %s AND to_place = %s

                    UNION

                    SELECT V.vehicle_code, date(Z.depart_time), "time"(Z.depart_time), Z.to_place, Z.from_place, Z.nbooked, V.capacity
                    FROM Journey Z JOIN Vehicle V USING (vehicle_code)
		                           JOIN Connection ON (Z.from_place = Connection.from_place) AND (Z.to_place = Connection.to_place)
                )
                SELECT *
                FROM Connection;"""

        cur.execute(sql, (from_place, to_place))
        journeys_db = cur.fetchall()

    except:
        con.rollback()
        return False

    cur.close()
    con.close()

    # TODO - get a list of all journeys between two places!
    # List all the journeys between two locations.
    # Should be chronologically ordered
    # It is a list of lists

    # Format:
    # [
    #   [ vehicle, day, time, to, from, nbooked, vehicle_capacity],
    #   ...
    # ]

    journeys = [{
        'vehicle': row[0],
        'start_day': row[1],
        'start_time': row[2],
        'to' : row[3],
        'from' : row[4],
        'booked' : row[5],
        'capacity' : row[6]
    } for row in journeys_db]

    return journeys


'''
Get all of the journeys for a given day, from and to a selected place.
'''
def get_day_journeys(from_place, to_place, journey_date):

    # TODO - update the journeys_db variable to get information from the database about this journey!
    # List all the journeys between two locations.
    # Should be chronologically ordered
    # It is a list of lists

    # Format:
    # [
    #   [ vehicle, day, time, to, from, nbooked, vehicle_capacity],
    #   ...
    # ]

    con = database_connect()
    if (con is None):
        return None
    cur = con.cursor()

    try:
        sql = """SELECT vehicle_code, date(depart_time), "time"(depart_time), to_place, from_place, capacity
                 FROM Journey JOIN Vehicle USING (vehicle_code)
                 WHERE from_place = %s AND to_place = %s AND date(depart_time) = %s
                 ORDER BY depart_time;"""

        cur.execute(sql, (from_place, to_place, journey_date))
        journeys_db = cur.fetchall()

    except:
        con.rollback()
        return None
    cur.close()
    con.close()

    journeys = [{
        'vehicle': row[0],
        'start_day': row[1],
        'start_time': row[2],
        'to': row[3],
        'from': row[4]
    } for row in journeys_db]

    return journeys



#####################################################
## Events
#####################################################

'''
List all the events running in the olympics
'''
def all_events():

    # TODO - update the events_db to get all events
    # Get all the events that are running.
    # Return the data (NOTE: look at the information, requires more than a simple select. NOTE ALSO: ordering of columns)
    # It is a list of lists
    # Chronologically order them by start

    # Format:
    # [
    #   [name, start, sport, venue_name]
    # ]

    con = database_connect()
    if (con is None):
        return None
    cur = con.cursor()

    try:
        sql = """SELECT event_name, "time"(event_start), sport_name, sport_venue, event_gender, event_id
                 FROM Event JOIN Sport USING (sport_id)
                 ORDER BY event_start;"""

        cur.execute(sql)
        events_db = cur.fetchall()

    except:
        con.rollback()
        return None
    cur.close()
    con.close()

    # events_db = [
    #     ['200M Freestyle', '0800', 'Swimming', 'Olympic Swimming Pools', 'M', '123'],
    #     ['1km Women\'s Cycle', '1800', 'Cycling', 'Velodrome', 'W', '001']
    # ]

    events = [{
        'name': row[0],
        'start': row[1],
        'sport': row[2],
        'venue': row[3],
        'gender': row[4],
        'event_id': row[5]
    } for row in events_db]

    return events


'''
Get all the events for a certain sport - list it in order of start
'''
def all_events_sport(sportname):

    # TODO - update the events_db to get all events for a particular sport
    # Get all events for sport name.
    # Return the data (NOTE: look at the information, requires more than a simple select. NOTE ALSO: ordering of columns)
    # It is a list of lists
    # Chronologically order them by start

    # Format:
    # [
    #   [name, start, sport, venue_name]
    # ]

    #events_db = [
    #    ['1km Women\'s Cycle', '1800', 'Cycling', 'Velodrome'],
    #    ['1km Men\'s Cycle', '1920', 'Cycling', 'Velodrome']
    #]

    con = database_connect()
    if (con is None):
        return None
    cur = con.cursor()

    try:
        sql = """SELECT event_name, "time"(event_start), sport_name, sport_venue
                 FROM Event JOIN Sport USING (sport_id)
                 WHERE sport_name = %s
                 ORDER BY event_start;"""

        cur.execute(sql, [sportname])
        events_db = cur.fetchall()

    except:
        con.rollback()
        return None
    cur.close()
    con.close()



    events = [{
        'name': row[0],
        'start': row[1],
        'sport': row[2],
        'venue': row[3],
    } for row in events_db]

    return events

'''
Get all of the events a certain member is participating in.
'''
def get_events_for_member(member_id): #won't be explicitly tested

    # TODO - update the events_db variable to pull from the database
    # Return the data (NOTE: look at the information, requires more than a simple select. NOTE ALSO: ordering of columns)
    # It is a list of lists
    # Chronologically order them by start

    # Format:
    # [
    #   [name, start, sport, venue_name]
    # ]

    #events_db = [
    #    ['1km Women\'s Cycle', '1800', 'Cycling', 'Velodrome', 'W'],
    #    ['1km Men\'s Cycle', '1920', 'Cycling', 'Velodrome', 'X']

    #]

    con = database_connect()
    if (con is None):
        return None
    cur = con.cursor()

    try:
         sql = """(SELECT DISTINCT event_name, "time"(event_start), sport_name, sport_venue, COALESCE(event_gender,'X') AS event_gender
                   FROM Event JOIN Sport USING (sport_id)
	               JOIN TeamMember USING (event_id)
                   WHERE athlete_id = %s)
                   UNION
                  (SELECT DISTINCT event_name, "time"(event_start), sport_name, sport_venue, COALESCE(event_gender,'X') AS event_gender
                   FROM Event JOIN Sport USING (sport_id)
	               JOIN Participates USING (event_id)
                   WHERE athlete_id = %s);"""
         cur.execute(sql, [member_id])
         events_db = cur.fetchall()

    except:
        con.rollback()
        return None
    cur.close()
    con.close()

    events = [{
        'name': row[0],
        'start': row[1],
        'sport': row[2],
        'venue': row[3],
        'gender': row[4]
    } for row in events_db]

    return events

'''
Get event information for a certain event
'''
def event_details(event_id):
    # TODO - update the event_db to get that particular event
    # Get all events for sport name.
    # Return the data (NOTE: look at the information, requires more than a simple select. NOTE ALSO: ordering of columns)
    # It is a list of lists
    # Chronologically order them by start

    # Format:
    #   [name, start, sport, venue_name]

    # event_db = ['1km Women\'s Cycle', '1800', 'Cycling', 'Velodrome']

    con = database_connect()
    if (con is None):
        return None
    cur = con.cursor()

    try:
        sql = """SELECT event_name, "time"(event_start), sport_name, sport_venue, event_gender
                 FROM Event JOIN Sport USING (sport_id)
                 WHERE event_id = %s
                 ORDER BY event_start;"""
        cur.execute(sql, [event_id])
        events_db = cur.fetchone()

    except:
        con.rollback()
        return None
    cur.close()
    con.close()

    event = {
        'name' : events_db[0],
        'start': events_db[1],
        'sport': events_db[2],
        'venue': events_db[3],
        'gender': events_db[4]
    }

    return event



#####################################################
## Results
#####################################################

'''
Get the results for a given event.
'''
def get_results_for_event(event_id):

    # TODO - update the results_db to get information from the database!
    # Return the data (NOTE: look at the information, requires more than a simple select. NOTE ALSO: ordering of columns)
    # This should return a list of who participated and the results.

    # This is a list of lists.
    # Order by ranking of medal, then by type (e.g. points/time/etc.)

    # Format:
    # [
    #   [member_id, result, medal],
    #   ...
    # ]

    con = database_connect()
    if (con is None):
        return None
    cur = con.cursor()

    try:
        sql = """SELECT event_id
                 FROM IndividualEvent
                 WHERE event_id = %s;"""
        cur.execute(sql, [event_id])

        if (cur.rowcount == 1):
             sql = """SELECT athlete_id, COALESCE(medal,'')
                      FROM Participates JOIN Member ON (athlete_id = member_id)
                      WHERE event_id = %s
                      ORDER BY
                      CASE WHEN medal = 'G' THEN 0
                           WHEN medal = 'S' THEN 1
                           WHEN medal = 'B' THEN 2
                           ELSE 3
                      END ASC, family_name, given_names;"""
             cur.execute(sql, [event_id])
             results_db = cur.fetchall()
        else:
            sql = """SELECT athlete_id, COALESCE(medal,'')
                     FROM Team JOIN TeamMember USING (event_id, team_name)
                               JOIN Member ON (athlete_id = member_id)
                     WHERE event_id = %s
                     ORDER BY
                     CASE WHEN medal = 'G' THEN 0
                          WHEN medal = 'S' THEN 1
                          WHEN medal = 'B' THEN 2
                          ELSE 3
                     END ASC, family_name, given_names;"""
            cur.execute(sql, [event_id])
            results_db = cur.fetchall()

    except:
        con.rollback()
        return None
    cur.close()
    con.close()

    #results_db = [
    #    ['1234567890', 'Gold'],
    #    ['8761287368', 'Silver'],
    #    ['1638712633', 'Bronze'],
    #    ['5873287436', ''],
    #    ['6328743234', '']
    #]

    results =[{
        'member_id': row[0],
        'medal': row[1]
    } for row in results_db]

    return results

'''
Get all the officials that participated, and their positions.
'''
def get_all_officials(event_name):
    # TODO
    # Return the data (NOTE: look at the information, requires more than a simple select. NOTE ALSO: ordering of columns)
    # This should return a list of who participated and their role.

    # This is a list of lists.

    # [
    #   [member_id, role],
    #   ...
    # ]


    con = database_connect()
    if (con is None):
        return None
    cur = con.cursor()

    try:
        sql = """SELECT member_id, role
                 FROM RunsEvent
                 WHERE event_id = %s;"""

        cur.execute(sql, [event_id])
        officials_db = cur.fetchall()

    except:
        con.rollback()
        return None
    cur.close()
    con.close()


    officials = [{
        'member_id': row[0],
        'role': row[1]
    } for row in officials_db]


    return officials

# =================================================================
# =================================================================

#  FOR MARKING PURPOSES ONLY
#  DO NOT CHANGE

def to_json(fn_name, ret_val):
    return {'function': fn_name, 'res': json.dumps(ret_val)}

# =================================================================
# =================================================================

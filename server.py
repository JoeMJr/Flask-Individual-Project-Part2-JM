from operator import and_
from flask import Flask, request
from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import text, func
from sqlalchemy import exc
from flask_cors import CORS
from flask_cors import cross_origin
import datetime

db = SQLAlchemy()

app = Flask(__name__)

#CORS(app, resources={r"/topfive": {"origins": "http://localhost:3000"}})
CORS(app, resources={r"/*": {"origins": "http://localhost:3000"}})

# The mySQL PART
app.config['SQLALCHEMY_DATABASE_URI'] = 'databaseanditspassowrd'

db.init_app(app)

class Customer(db.Model):
    __tablename__ = 'customer'

    customer_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    first_name = db.Column(db.String(50), nullable=False)
    last_name = db.Column(db.String(50), nullable=False)
    email = db.Column(db.String(100), nullable=False, unique=True)
    store_id = db.Column(db.Integer, nullable=False)
    address_id = db.Column(db.Integer, nullable=False)
    active = db.Column(db.Boolean, default=True)
    create_date = db.Column(db.DateTime, default=db.func.current_timestamp())
    last_update = db.Column(db.DateTime, default=db.func.current_timestamp(), onupdate=db.func.current_timestamp())

class Rental(db.Model):
    __tablename__ = 'rental'
    rental_id = db.Column(db.Integer, primary_key=True)
    rental_date = db.Column(db.DateTime)
    inventory_id = db.Column(db.Integer, db.ForeignKey('inventory.inventory_id'))
    customer_id = db.Column(db.Integer, db.ForeignKey('customer.customer_id'))
    return_date = db.Column(db.DateTime)
    staff_id = db.Column(db.Integer, db.ForeignKey('staff.staff_id'))  # Add this field

    inventory = db.relationship('Inventory', backref='rentals')
    customer = db.relationship('Customer', backref='rentals')

class Inventory(db.Model):
    __tablename__ = 'inventory'
    inventory_id = db.Column(db.Integer, primary_key=True)
    film_id = db.Column(db.Integer, db.ForeignKey('film.film_id'))
    last_update = db.Column(db.DateTime, default=datetime.datetime.now())

class Film(db.Model):
    __tablename__ = 'film'
    film_id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(255))

class Staff(db.Model):
    __tablename__ = 'staff'
    staff_id = db.Column(db.Integer, primary_key=True)
    first_name = db.Column(db.String(45))
    last_name = db.Column(db.String(45))
    email = db.Column(db.String(50))
    store_id = db.Column(db.Integer, db.ForeignKey('store.store_id'))  # Assuming store_id exists
    active = db.Column(db.Boolean)
    username = db.Column(db.String(16))
    password = db.Column(db.String(40))
    last_update = db.Column(db.DateTime)

#def check():
    #return 'Flask is working!!!!!!!11!!'
@app.route('/')
def testdb():
    try:
        db.session.query(text('1')).from_statement(text('SELECT 1')).all()
        return '<h1>It works.</h1>'
    except Exception as e:
        # e holds description of the error
        error_text = "<p>The error:<br>" + str(e) + "</p>"
        hed = '<h1>Something is broken.</h1>'
        return hed + error_text

@app.route('/topfive', methods=['GET'])
def gettopfive():
    # Top five films i know that I made this non descriptive
    # THIS WORKS :D
    query = text("""
        select Q.film_id, title, count(T.inventory_id) as rented
        from sakila.film Q
        join sakila.inventory S on (Q.film_id = S.film_id)
        join sakila.rental T on (S.inventory_id = T.inventory_id)
        group by Q.film_id
        order by rented DESC
        limit 5; 
        """)

    # Execute the query with a parameter
    result = db.session.execute(query, {'status': 'active'})
    #print(type(result))
    
    # Convert the result to a list of dictionaries
    columns = result.keys()  # Get column names
    topfivelist = [dict(zip(columns, row)) for row in result.fetchall()]
    return jsonify(topfivelist)  # Return the result as a string or render it in a template


@app.route('/filminfo/<int:film_id>', methods=['GET'])
def getfilminfo(film_id):
    my_var = request.args.get('variable')
    #if not my_var:
        #return 'No variable passed!', 400  # Return an error if variable is not passed
    
    # Perform a raw SQL query using db.session.execute
    query = text('''
    select A.film_id, title, release_year, rating, name as FilmCategory, description
    from sakila.film A
    left outer join sakila.film_category B on (A.film_id = B.film_id)
    left outer join sakila.category C on (B.category_id = C.category_id)
    where A.film_id = :film_id
    order by B.category_id;
    ''')

    result = db.session.execute(query, {'film_id': film_id})

    #print("TEST: ",type(result))

    if not result:
        return jsonify({'error': 'Film not found'}), 404  # Handle case where no result is found

    columns = result.keys()  # Get column names
    result_list = [dict(zip(columns, row)) for row in result.fetchall()]

    return jsonify(result_list)

@app.route('/topactor', methods=['GET'])
def gettopactor():
    queryA = text('''
    select distinct F.actor_id, first_name, last_name, count(title) as movies
    from sakila.actor F
    left outer join sakila.film_actor G on (F.actor_id = G.actor_id)
    left outer join sakila.film H on (G.film_id = H.film_id)
    where F.actor_id is not NULL or H.film_id is not NULL
    group by F.actor_id
    order by movies DESC
    limit 5;
    ''')
    result = db.session.execute(queryA, {'status': 'active'})
    columns = result.keys()  # Get column names
    topfivelist = [dict(zip(columns, row)) for row in result.fetchall()]


    #topfivelist = [dict(row.items()) for row in result.fetchall()]

    #for row in result.fetchall(): 
        #someMovie = {'Film Id': row['film_id'], 'title': row['title'], 'Rental Count': row['rented']}
        #topfivelist.append(someMovie)

    # Fetch all the results
    #customers = result.fetchall()
    return jsonify(topfivelist)  # Return the result as a string or render it in a template

@app.route('/actormovies/<int:actor_id>', methods=['GET'])
def getactormovies(actor_id):
    my_var = request.args.get('variable')

    queryB = text('''
    select N.film_id, L.title, count(P.inventory_id) as rental_count 
    from (select actor_id, count(film_id) as test
    from film_actor
    group by actor_id
    order by test DESC
    limit 1) as M
    join (select actor_id, film_id
    from film_actor) as N
    inner join film L on (N.film_id = L.film_id)
    join sakila.inventory O on (L.film_id = O.film_id)
    join sakila.rental P on (O.inventory_id = P.inventory_id)
    where N.actor_id and N.actor_id = :actor_id
    group by N.film_id
    order by rental_count DESC
    limit 5;
    ''')

    result = db.session.execute(queryB, {'actor_id': actor_id})

    #print("TEST: ",type(result))

    if not result:
        return jsonify({'error': 'Actor not found'}), 404  # Handle case where no result is found

    columns = result.keys()  # Get column names
    topfivelist = [dict(zip(columns, row)) for row in result.fetchall()]


    #topfivelist = [dict(row.items()) for row in result.fetchall()]

    #for row in result.fetchall(): 
        #someMovie = {'Film Id': row['film_id'], 'title': row['title'], 'Rental Count': row['rented']}
        #topfivelist.append(someMovie)

    # Fetch all the results
    #customers = result.fetchall()
    return jsonify(topfivelist)  # Return the result as a string or render it in a template



@app.route('/customer', methods=['GET'])
def get_customer():
    try:
        page = int(request.args.get('page', 1))  # Default to page 1
        per_page = int(request.args.get('per_page', 3))  # Default to 3 items per page
        # Get search parameters
        customer_id_search = request.args.get('customer_id', '')
        first_name_search = request.args.get('first_name', '')
        last_name_search = request.args.get('last_name', '')
        
        print("ID:(", customer_id_search, "), F:(", first_name_search, "), L: (", last_name_search, ")")

        # Build the base query
        queryC = text("SELECT * FROM customer WHERE 1=1")  # Ensure there's at least a WHERE clause to start the query

        # Only add conditions if the corresponding search term is not empty
        query_params = {}

        conditions = [text("1=1")]  # Start with the base condition
        # ::TEXT
        if customer_id_search:
            conditions.append(text("LOWER(customer_id) LIKE :customer_id"))
            query_params['customer_id'] = f"%{customer_id_search.upper()}%"

        if first_name_search:
            conditions.append(text("LOWER(first_name) LIKE :first_name"))
            query_params['first_name'] = f"%{first_name_search.upper()}%"

        if last_name_search:
            conditions.append(text("LOWER(last_name) LIKE :last_name"))
            query_params['last_name'] = f"%{last_name_search.upper()}%"

        # Now combine conditions with AND
        
        if all(not var for var in [customer_id_search, first_name_search, last_name_search]):
            print("All variables are empty")
            final_condition = conditions[0]
        else:
            print("Not All variables are empty")
            if len(conditions) == 3:
                print("Test")
                final_condition = and_(conditions[0], and_(conditions[1], conditions[2]))
            elif len(conditions) == 4:
                final_condition = and_(conditions[0], and_(conditions[1], and_(conditions[2], conditions[3])))
            else:
                final_condition = and_(*conditions)

        # Use the final_condition in the query
        queryC = text(f"SELECT * FROM customer WHERE {final_condition}")
        
        # Execute the query with the parameters
        print("The Query?(", queryC, ") PLS actually help")
        print("The args?(", query_params, ") PLS actually help")
        result = db.session.execute(queryC, query_params)
        
        columns = result.keys()  # Get column names
        customers = [dict(zip(columns, row)) for row in result.fetchall()]
        
        start = (page - 1) * per_page
        end = start + per_page
        paginated_customers = customers[start:end]
        
        return jsonify({
            'customers': paginated_customers,
            'total': len(customers),
            'page': page,
            'per_page': per_page
        })
    
    except Exception as e:
        # Catch any error and return a JSON response with the error message
        print("error:", str(e))
        return jsonify({"error": str(e)}), 500
    #return jsonify(customers)  # Return as string or render template with results

#

@app.route('/addcustomer', methods=['POST'])
def add_customer():
    try:
        # Get customer data from the request body
        data = request.get_json()

        # Validate data (optional, depending on your requirements)
        if not data.get('first_name') or not data.get('last_name') or not data.get('email'):
            return jsonify({"error": "Missing required fields: first_name, last_name, email"}), 400
        # NOTED - Add capitalization to the name and email
        # Create a new customer instance
        new_customer = Customer(
            first_name=data['first_name'],
            last_name=data['last_name'],
            email=data['email'],
            store_id=data['store_id'],
            address_id=data['address_id'],
            active=data.get('active', True),  # Default to active if not specified
            create_date=data.get('create_date', None),  # Optional create_date
            last_update=data.get('last_update', None)  # Optional last_update
        )

        # Add to the database
        db.session.add(new_customer)
        db.session.commit()  # Save the new customer to the database

        # Return a success response
        return jsonify({"message": "Customer added successfully!"}), 201

    except Exception as e:
        # Rollback any changes if there's an error
        db.session.rollback()
        return jsonify({"error": str(e)}), 500
#
#
#
@app.route('/upcustomer/<int:customer_id>', methods=['PATCH'])
def update_customer(customer_id):
    try:
        # Get the data sent from the frontend
        print("I was alive?")
        data = request.get_json()

        # Debug: print out the incoming data to ensure it's being received correctly
        print("Received data:", data)

        # Find the customer by ID
        customer = Customer.query.get(customer_id)

        if not customer:
            return jsonify({"error": "Customer not found"}), 404

        # Debug: print the current customer details to check the existing data
        print("Current customer:", customer)

        # NOTED - Add capitalization to the name and email again
        # Update the fields that are provided in the request body
        updated_fields = False
        if 'firstName' in data:
            print("1")
            customer.first_name = data['firstName']
            updated_fields = True
        if 'lastName' in data:
            print("2")
            customer.last_name = data['lastName']
            updated_fields = True
        if 'email' in data:
            print("3")
            customer.email = data['email']
            updated_fields = True
        if 'storeId' in data:
            print("4")
            customer.store_id = data['storeId']
            updated_fields = True
        if 'addressId' in data:
            print("5")
            customer.address_id = data['addressId']
            updated_fields = True
        if 'active' in data:
            print("6")
            customer.active = data['active']
            updated_fields = True

        # Only update if fields have actually changed
        if updated_fields:
            # Automatically update the last_update timestamp
            customer.last_update = db.func.current_timestamp()

            # Debug: print the updated customer details
            print("Updated customer:", customer)

            # Commit the changes to the database
            db.session.commit()
            print("Updated data:", customer.store_id, "add:", customer.address_id)
            print("Updated customer:", customer.store_id, "add:", customer.address_id)
            
            return jsonify({"message": "Customer updated successfully!"}), 200
        else:
            return jsonify({"message": "No fields were updated."}), 400

    except Exception as e:
        # Rollback the session in case of error
        db.session.rollback()
        print("Error:", str(e))
        return jsonify({"error": str(e)}), 500
#
#
#
@app.route('/deletecustomer/<int:customer_id>', methods=['DELETE'])
def delete_customer(customer_id):
    try:
        # Find the customer by ID
        customer = Customer.query.get(customer_id)
        
        if not customer:
            return jsonify({'error': 'Customer not found'}), 404
        
        # Delete the customer
        db.session.delete(customer)
        db.session.commit()

        return jsonify({'message': f'Customer {customer_id} deleted successfully'}), 200

    except exc.SQLAlchemyError as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500
#
#
#
@app.route('/search', methods=['GET'])
def search():
    film_movie_title = request.args.get('movie_title')
    actor_first_name = request.args.get('first_name')
    actor_last_name = request.args.get('last_name')
    film_movie_genre = request.args.get('movie_genre')

    query = """
    SELECT 
        f.film_id,  -- Include film_id here
        f.title AS film_title,
        GROUP_CONCAT(CONCAT(a.first_name, ' ', a.last_name) ORDER BY a.last_name) AS actor_names,
        c.name AS category_name
    FROM 
        film f
    JOIN 
        film_actor fa ON f.film_id = fa.film_id
    JOIN 
        actor a ON fa.actor_id = a.actor_id
    JOIN 
        film_category fc ON f.film_id = fc.film_id
    JOIN 
        category c ON fc.category_id = c.category_id
    WHERE 
        a.first_name LIKE :first_name AND a.last_name LIKE :last_name AND f.title LIKE :movie_title AND c.name LIKE :movie_genre
    GROUP BY 
        f.film_id, c.category_id;
    """

    # Execute the query using db.session.execute with parameterized values
    result = db.session.execute(text(query), {'first_name': f'%{actor_first_name}%', 'last_name': f'%{actor_last_name}%', 'movie_title': f'%{film_movie_title}%', 'movie_genre': f'%{film_movie_genre}%'})

    # Fetch the results as a list of dictionaries
    films = result.fetchall()

    # Convert the results into a list of dictionaries to return as JSON
    films_data = [
        {
            'film_id': film[0],  # Include film_id
            'film_title': film[1],
            'actor_names': film[2],
            'category_name': film[3]
        } for film in films
    ]

    return jsonify(films_data)
#
#
#
@app.route('/filmdet/<int:film_id>', methods=['GET'])
def get_film_details(film_id):
    print("TEST FOR DETS,", film_id)
    query = text("""
    SELECT 
        f.title AS film_title,
        f.description,
        f.release_year,
        GROUP_CONCAT(CONCAT(a.first_name, ' ', a.last_name) ORDER BY a.last_name) AS actor_names,
        GROUP_CONCAT(c.name ORDER BY c.name) AS categories
    FROM 
        film f
    LEFT JOIN 
        film_actor fa ON f.film_id = fa.film_id
    LEFT JOIN 
        actor a ON fa.actor_id = a.actor_id
    LEFT JOIN 
        film_category fc ON f.film_id = fc.film_id
    LEFT JOIN 
        category c ON fc.category_id = c.category_id
    WHERE 
        f.film_id = :film_id
    GROUP BY 
        f.film_id;
    """)

    # Execute the query with the film_id
    result = db.session.execute(query, {'film_id': film_id})

    # Fetch the result
    film_details = result.fetchone()

    if film_details:
        film_data = {
            'film_title': film_details[0],
            'description': film_details[1],
            'release_year': film_details[2],
            'actor_names': film_details[3],
            'categories': film_details[4]
        }
        return jsonify(film_data)
    else:
        return jsonify({'error': 'Film not found'}), 404
#
#
#
@app.route('/rent-film', methods=['POST'])
@cross_origin(origins="http://localhost:3000")
def rent_film():
    customer_id = request.json.get('customer_id')
    film_title = request.json.get('film_title')

    # Step 1: Find an available inventory item for the film
    film = Film.query.filter_by(title=film_title).first()

    if not film:
        return jsonify({"error": "Film not found"}), 404
    # NOTED - Add return date thing
    available_inventory = Inventory.query.join(Rental, Rental.inventory_id == Inventory.inventory_id) \
                                           .filter(Inventory.film_id == film.film_id) \
                                           .filter(Rental.return_date != None).first()

    if not available_inventory:
        return jsonify({"error": "No available copies of the film"}), 400

    # Step 2: Insert a rental record
    rental = Rental(
        rental_date=datetime.datetime.now(),
        inventory_id=available_inventory.inventory_id,
        customer_id=customer_id,
        return_date=None,  # Not returned yet
        staff_id=1  # Placeholder
    )

    db.session.add(rental)
    db.session.commit()

    return jsonify({"message": "Film rented successfully!"}), 201
#
#
#
@app.route('/customer-rent-details/<int:customer_id>', methods=['GET'])
def customer_rent_details(customer_id):
    # Find the customer based on customer_id
    print("I WAS HERE!2!")
    print("cust id?", customer_id)
    queryD = text('''
    SELECT 
    c.customer_id,
    f.title AS movie_title,
    r.rental_date,
    r.return_date,
    CASE 
        WHEN r.return_date IS NULL THEN 'Present Rental (Not Returned)'
        ELSE 'Past Rental (Returned)'
    END AS rental_status
    FROM 
        rental r
    JOIN 
        customer c ON r.customer_id = c.customer_id
    JOIN 
        inventory i ON r.inventory_id = i.inventory_id
    JOIN 
        film f ON i.film_id = f.film_id
    WHERE 
        c.customer_id = :customer_id  -- Replace with the specific customer ID
    ORDER BY 
        r.rental_date DESC;  -- Optional: orders by most recent rental first
    ''')

    result = db.session.execute(queryD, {'customer_id': customer_id})

    print("TEST: ",type(result))

    if not result:
        return jsonify({'error': 'Customer not found'}), 404  # Handle case where no result is found

    columns = result.keys()  # Get column names
    rentdets = [dict(zip(columns, row)) for row in result.fetchall()]

    return jsonify(rentdets)  # Return the result as a string or render it in a template
#
#
#
@app.route('/customer-rent-return', methods=['PATCH'])
def customer_rent_return():
    '''
    try:
        customer_id = request.json.get('customer_id')
        film_title = request.json.get('movie_title')

        print("Cust Id:", str(customer_id) + ", Film: ", film_title)

        if not customer_id or not film_title:
            return jsonify({"error": "customer_id and film_title are required."}), 400
        
        testval = str(customer_id) + ": (" + film_title + ")"
        print("This happened:", str(testval))
        return jsonify({"I worked": str(testval)}), 200
    except Exception as e:
        # Rollback the session in case of error
        db.session.rollback()
        print("Error:", str(e))
        return jsonify({"error": str(e)}), 500'
    '''
    try:
        # Get customer_id and film_title from the request
        customer_id = request.json.get('customer_id')
        film_title = request.json.get('movie_title')

        if not customer_id or not film_title:
            return jsonify({"error": "customer_id and film_title are required."}), 400

        # Find the rental record that matches the customer and film
        rental = db.session.query(Rental).join(Inventory).join(Film).filter(
            Rental.customer_id == customer_id,
            Film.title == film_title,
            Rental.return_date.is_(None)  # Active rental not returned yet
        ).first()

        if not rental:
            return jsonify({"error": "No active rental found for this customer and film."}), 404

        # Update return_date
        rental.return_date = datetime.datetime.now()

        # Update inventory last_update
        inventory = Inventory.query.filter_by(inventory_id=rental.inventory_id).first()
        if inventory:
            inventory.last_update = datetime.datetime.now()

        # Commit the transaction
        db.session.commit()

        return jsonify({"message": f"Film '{film_title}' returned successfully by customer {customer_id}."}), 200

    except Exception as e:
        db.session.rollback()
        print("Error:", str(e))
        return jsonify({"error": str(e)}), 500

#
#
#
# Shitter test for React Fetch
x = datetime.datetime.now()
@app.route('/data')
def get_time():
    # Returning an api for showing in reactjs
    return {
        'Name':"geek", 
        "Age":"22",
        "Date":x, 
        "programming":"python"
        }


if __name__ == '__main__':
    app.run(debug=True)

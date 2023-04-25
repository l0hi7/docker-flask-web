from flask import Flask, jsonify, render_template,request
from ndmspostgres import NDMSDBMSUpdate


import os

app = Flask(__name__)


@app.route('/')
def home():
    return render_template('login.html')



@app.route('/vm_mustang/login')
def rehome():
    return render_template('login.html')

@app.route('/vm_mustang/User')
def vm_mustanguser():
    return render_template("vm_mustang.html")

@app.route('/vm_mustang/Admin')
def vm_mustangadmin():
    return render_template("vm_mustang.html")

@app.route('/vm_mustang/management')
def vm_mustangmanagement():
    return render_template("vm_project_management.html")

@app.route('/userdetails',methods=['GET','POST'])
def get_userdetails():
    email=request.args.get('email')
    print(email)
    userlist=NDMSDBMSUpdate().userauthtable(email)
    # print(userlist)
    return jsonify(response =userlist)

@app.route('//mustanguserdata/<userfirstname>',methods=['GET','POST'])
def get_useritems(userfirstname):
    print(userfirstname)
    userlist=NDMSDBMSUpdate().getusertable(userfirstname)
    # print(userlist)
    return jsonify(response =userlist)

@app.route('/mustangalldata',methods=['GET','POST'])
def get_items():
    items=NDMSDBMSUpdate().getall()
    return jsonify(items)

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 4750))
    app.run(debug=True, host='0.0.0.0', port=port)
    app.static_folder = 'static'







# @app.route('/items/<id>', methods=['PUT'])
# def update_item(id):
#     body = request.get_json()
#     db.session.query(Item).filter_by(id=id).update(
#         dict(title=body['title'], content=body['content']))
#     db.session.commit()
#     return "item updated"


# @app.route('/items/<id>', methods=['DELETE'])
# def delete_item(id):
#     db.session.query(Item).filter_by(id=id).delete()
#     db.session.commit()
#     return "item deleted"


# @app.route('/accounts/', methods=['POST'])
# def create_user():
#     """Create an account."""
#     data = request.get_json()
#     name = data['name']
#     if name:
#         new_account = Account(name=name,
#                             created_at=dt.now())
#         db.session.add(new_account)  # Adds new User record to database
#         db.session.commit()  # Commits all changes
#         return make_response(f"{new_account} successfully created!")
#     else:
#         return make_response(f"Name can't be null!")
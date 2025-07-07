from flask import Flask, request, redirect, url_for, render_template, session, flash
from log_utils import load_config, init_db, parse_and_store, get_logs, register_user, validate_user
from functools import wraps

app = Flask(__name__)
app.secret_key = 'very-secret'

cfg = load_config()
db_path = init_db(cfg['Database']['URI'])

# Декоратор для защищённых маршрутов

def login_required(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return wrapped

# Регистрация пользователя
@app.route('/register', methods=['GET','POST'])
def register():
    if request.method == 'POST':
        u = request.form['username']
        p = request.form['password']
        if register_user(db_path, u, p):
            flash('Регистрация успешна')
            return redirect(url_for('login'))
        else:
            flash('Имя занято')
    return render_template('register.html')

# Вход
@app.route('/login', methods=['GET','POST'])
def login():
    if request.method == 'POST':
        u = request.form['username']
        p = request.form['password']
        if validate_user(db_path, u, p):
            session['user'] = u
            return redirect(url_for('dashboard'))
        else:
            flash('Неверные данные')
    return render_template('login.html')

# Панель управления
@app.route('/')
@login_required
def dashboard():
    return render_template('dashboard.html', user=session['user'])

# Парсинг
@app.route('/parse')
@login_required
def web_parse():
    ok, res = parse_and_store(db_path, cfg['Logs']['Directory'], cfg['Logs']['Pattern'])
    flash(f'Добавлено записей: {res}')
    return redirect(url_for('dashboard'))

# Просмотр логов
@app.route('/show', methods=['GET'])
@login_required
def web_show():
    filt = {k: request.args.get(k) for k in ('ip','keyword','date_from','date_to')}
    filt['limit'] = int(request.args.get('limit',100))
    rows = get_logs(db_path, filt)
    return render_template('show.html', rows=rows)

# Выход
@app.route('/logout')
def logout():
    session.pop('user', None)
    return redirect(url_for('login'))

if __name__ == '__main__':
    app.run(debug=True, port=5000)
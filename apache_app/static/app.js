document.addEventListener('DOMContentLoaded', function() {
  const form = document.querySelector('form');
  if (form) {
    form.addEventListener('submit', function(e) {
      const pw = form.querySelector('input[type=password]');
      if (pw && pw.value.length < 4) {
        alert('Пароль должен быть минимум 4 символа!');
        e.preventDefault();
      }
    });
  }
});
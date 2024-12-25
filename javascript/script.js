const button = document.getElementById('clickMe');
const message = document.getElementById('message');
button.addEventListener('click', () => {
    message.textContent = 'Hello from JavaScript!';
});

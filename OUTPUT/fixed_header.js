window.addEventListener('scroll', () => {
  Array.prototype.map.call(
    document.querySelectorAll('g.station > text'),
    (e) => e.setAttribute('x', window.pageXOffset)
  );
  Array.prototype.map.call(
    document.querySelectorAll('g.hour > text'),
    (e) => e.setAttribute('y', window.pageYOffset + 30)
  );
})

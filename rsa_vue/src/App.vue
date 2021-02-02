<template>
  <div id='app'>
      <nav>
        <div class='navigation__name'>
          <strong class='navigation__name-emph'>S</strong>tocks 
          <strong class='navigation__name-emph'>R</strong>edddit thinks 
          <strong class='navigation__name-emph'>A</strong>bout
        </div>
      </nav>
      <hr>
  </div>
</template>

<script>
  export default {
    name: 'App',
    components: {},
    data: () => ({
      title: 'Stocks Reddit thinks about'
    })
  };
</script>

<style lang='scss'>
#app {
  font-family: Avenir, Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;

  nav {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 10px 5%;
    background-color: #2c3e50;
    color: white;

    .navigation__name {
      font-weight: bold;

      .navigation__name-emph {
        color: orangered;
        font-size: 20px;
      }
    }
  }
}
</style>

<template>
  <div id='app'>
      <nav>
        <div class='navigation__name'>
          <strong class='navigation__logo-emph'>S</strong>tocks 
          <strong class='navigation__logo-emph'>R</strong>edddit thinks 
          <strong class='navigation__logo-emph'>A</strong>bout
        </div>
        <div class='navigation__logo'>
          logo placeholder
        </div>
      </nav>
      <hr>
  </div>
</template>

<script>
export default {
  name: 'App',
  data: () => ({
    title: 'Stocks Reddit thinks about'
  })
}
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

      .navigation__logo-emph {
        color: orangered;
        font-size: 20px;
      }
    }

    .navigation__logo {
      font-weight: bold;
    }
  }

}
</style>

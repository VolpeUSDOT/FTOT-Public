




<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
  <link rel="dns-prefetch" href="https://github.githubassets.com">
  <link rel="dns-prefetch" href="https://avatars0.githubusercontent.com">
  <link rel="dns-prefetch" href="https://avatars1.githubusercontent.com">
  <link rel="dns-prefetch" href="https://avatars2.githubusercontent.com">
  <link rel="dns-prefetch" href="https://avatars3.githubusercontent.com">
  <link rel="dns-prefetch" href="https://github-cloud.s3.amazonaws.com">
  <link rel="dns-prefetch" href="https://user-images.githubusercontent.com/">



  <link crossorigin="anonymous" media="all" integrity="sha512-QV1ZNBjZz8stPx+uh4ZAKc6AJ1z8A9VHut/SGtgbc+iYLfhrh68QmDH3rZgkXJ0BWIOcDw+ILnWcctH0ljcHPg==" rel="stylesheet" href="https://github.githubassets.com/assets/frameworks-415d593418d9cfcb2d3f1fae87864029.css" />
  
    <link crossorigin="anonymous" media="all" integrity="sha512-N96W0hcNNyCuKN+3qLze6ikDW2+qwrPm3zPGyXs5XySePJ65ZyrDUoMvjNKSXa7LiIUzx/uUK8LsJ0OzaYUDpw==" rel="stylesheet" href="https://github.githubassets.com/assets/github-37de96d2170d3720ae28dfb7a8bcdeea.css" />
    
    
    
    


  <meta name="viewport" content="width=device-width">
  
  <title>FTOT/ftot_routing.py at master · VolpeUSDOT/FTOT</title>
    <meta name="description" content="Freight Transportation Optimization Tool (US DOT Volpe Internal Version) - VolpeUSDOT/FTOT">
    <link rel="search" type="application/opensearchdescription+xml" href="/opensearch.xml" title="GitHub">
  <link rel="fluid-icon" href="https://github.com/fluidicon.png" title="GitHub">
  <meta property="fb:app_id" content="1401488693436528">
  <meta name="apple-itunes-app" content="app-id=1477376905">

    <meta name="twitter:image:src" content="https://avatars2.githubusercontent.com/u/29147435?s=400&amp;v=4" /><meta name="twitter:site" content="@github" /><meta name="twitter:card" content="summary" /><meta name="twitter:title" content="VolpeUSDOT/FTOT" /><meta name="twitter:description" content="Freight Transportation Optimization Tool (US DOT Volpe Internal Version) - VolpeUSDOT/FTOT" />
    <meta property="og:image" content="https://avatars2.githubusercontent.com/u/29147435?s=400&amp;v=4" /><meta property="og:site_name" content="GitHub" /><meta property="og:type" content="object" /><meta property="og:title" content="VolpeUSDOT/FTOT" /><meta property="og:url" content="https://github.com/VolpeUSDOT/FTOT" /><meta property="og:description" content="Freight Transportation Optimization Tool (US DOT Volpe Internal Version) - VolpeUSDOT/FTOT" />



  

  <link rel="assets" href="https://github.githubassets.com/">
    <link rel="shared-web-socket" href="wss://alive.github.com/_sockets/u/45362680/ws?session=eyJ2IjoiVjMiLCJ1Ijo0NTM2MjY4MCwicyI6NTgxMDc3NDA1LCJjIjoyOTcyOTMxNzAxLCJ0IjoxNjAyNjEyMDIzfQ==--ca9203fe5c6532dd40a0da4a6035a27a454e9f4ed2291658ae17af7c2f641104" data-refresh-url="/_alive">
  <link rel="sudo-modal" href="/sessions/sudo_modal">

  <meta name="request-id" content="F769:42DE:2BD42FC:401E0AF:5F85EB2E" data-pjax-transient="true" /><meta name="html-safe-nonce" content="d4c82be28a818990ea5d55789e9aa62988eb01dd" data-pjax-transient="true" /><meta name="visitor-payload" content="eyJyZWZlcnJlciI6Imh0dHBzOi8vZ2l0aHViLmNvbS9Wb2xwZVVTRE9UL0ZUT1QvdHJlZS9tYXN0ZXIvcHJvZ3JhbSIsInJlcXVlc3RfaWQiOiJGNzY5OjQyREU6MkJENDJGQzo0MDFFMEFGOjVGODVFQjJFIiwidmlzaXRvcl9pZCI6IjE1NTc4NTQxODgyNjQ0Nzc5MjYiLCJyZWdpb25fZWRnZSI6ImlhZCIsInJlZ2lvbl9yZW5kZXIiOiJpYWQifQ==" data-pjax-transient="true" /><meta name="visitor-hmac" content="1b8f04de287e3305aa50f9ee4928ee3cc11729414c858cfe37ff0f68eea70501" data-pjax-transient="true" /><meta name="cookie-consent-required" content="false" />

    <meta name="hovercard-subject-tag" content="repository:162637563" data-pjax-transient>


  <meta name="github-keyboard-shortcuts" content="repository,source-code" data-pjax-transient="true" />

  

  <meta name="selected-link" value="repo_source" data-pjax-transient>

    <meta name="google-site-verification" content="c1kuD-K2HIVF635lypcsWPoD4kilo5-jA_wBFyT4uMY">
  <meta name="google-site-verification" content="KT5gs8h0wvaagLKAVWq8bbeNwnZZK1r1XQysX3xurLU">
  <meta name="google-site-verification" content="ZzhVyEFwb7w3e0-uOTltm8Jsck2F5StVihD0exw2fsA">
  <meta name="google-site-verification" content="GXs5KoUUkNCoaAZn7wPN-t01Pywp9M3sEjnt_3_ZWPc">

  <meta name="octolytics-host" content="collector.githubapp.com" /><meta name="octolytics-app-id" content="github" /><meta name="octolytics-event-url" content="https://collector.githubapp.com/github-external/browser_event" /><meta name="octolytics-dimension-ga_id" content="" class="js-octo-ga-id" /><meta name="octolytics-actor-id" content="45362680" /><meta name="octolytics-actor-login" content="matthewpearlson" /><meta name="octolytics-actor-hash" content="6009c0b4fa07ec1c9e6623fecbfe4e11702c8bf5460291cd1ca9c74a80eea9f1" />

  <meta name="analytics-location" content="/&lt;user-name&gt;/&lt;repo-name&gt;/blob/show" data-pjax-transient="true" />

  





    <meta name="google-analytics" content="UA-3769691-2">

  <meta class="js-ga-set" name="userId" content="393fe8ac0a6945c02a9615a5178ca904">

<meta class="js-ga-set" name="dimension10" content="Responsive" data-pjax-transient>

<meta class="js-ga-set" name="dimension1" content="Logged In">



  

      <meta name="hostname" content="github.com">
    <meta name="user-login" content="matthewpearlson">


      <meta name="expected-hostname" content="github.com">

      <meta name="js-proxy-site-detection-payload" content="MTFiNzA4MWZiOTQ1M2YyMjM4YjNlMzdhYjcyNTQwMzE5NWQ4YzdiNzIwNTFmYzNmNTkzYWY5MWUyYWZlNDBmMXx7InJlbW90ZV9hZGRyZXNzIjoiMTg0LjU4LjE1Ni4xOTEiLCJyZXF1ZXN0X2lkIjoiRjc2OTo0MkRFOjJCRDQyRkM6NDAxRTBBRjo1Rjg1RUIyRSIsInRpbWVzdGFtcCI6MTYwMjYxMjAyMywiaG9zdCI6ImdpdGh1Yi5jb20ifQ==">

    <meta name="enabled-features" content="MARKETPLACE_PENDING_INSTALLATIONS,JS_HTTP_CACHE_HEADERS">

  <meta http-equiv="x-pjax-version" content="985c0515c2e31356aee0e05278263338c20cd0ccfbcdc740fb095d96371fa102">
  

        <link href="https://github.com/VolpeUSDOT/FTOT/commits/master.atom?token=AK2C36ACROYWKABWNWX446F5SHNEO" rel="alternate" title="Recent Commits to FTOT:master" type="application/atom+xml">

  <meta name="go-import" content="github.com/VolpeUSDOT/FTOT git https://github.com/VolpeUSDOT/FTOT.git">

  <meta name="octolytics-dimension-user_id" content="29147435" /><meta name="octolytics-dimension-user_login" content="VolpeUSDOT" /><meta name="octolytics-dimension-repository_id" content="162637563" /><meta name="octolytics-dimension-repository_nwo" content="VolpeUSDOT/FTOT" /><meta name="octolytics-dimension-repository_public" content="false" /><meta name="octolytics-dimension-repository_is_fork" content="false" /><meta name="octolytics-dimension-repository_network_root_id" content="162637563" /><meta name="octolytics-dimension-repository_network_root_nwo" content="VolpeUSDOT/FTOT" /><meta name="octolytics-dimension-repository_explore_github_marketplace_ci_cta_shown" content="true" />


    <link rel="canonical" href="https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py" data-pjax-transient>


  <meta name="browser-stats-url" content="https://api.github.com/_private/browser/stats">

  <meta name="browser-errors-url" content="https://api.github.com/_private/browser/errors">

  <link rel="mask-icon" href="https://github.githubassets.com/pinned-octocat.svg" color="#000000">
  <link rel="alternate icon" class="js-site-favicon" type="image/png" href="https://github.githubassets.com/favicons/favicon.png">
  <link rel="icon" class="js-site-favicon" type="image/svg+xml" href="https://github.githubassets.com/favicons/favicon.svg">

<meta name="theme-color" content="#1e2327">


  <link rel="manifest" href="/manifest.json" crossOrigin="use-credentials">

  </head>

  <body class="logged-in env-production page-responsive page-blob">
    

    <div class="position-relative js-header-wrapper ">
      <a href="#start-of-content" class="p-3 bg-blue text-white show-on-focus js-skip-to-content">Skip to content</a>
      <span class="progress-pjax-loader width-full js-pjax-loader-bar Progress position-fixed">
    <span style="background-color: #79b8ff;width: 0%;" class="Progress-item progress-pjax-loader-bar "></span>
</span>      
      


          <header class="Header js-details-container Details px-3 px-md-4 px-lg-5 flex-wrap flex-md-nowrap" role="banner" >
    <div class="Header-item mt-n1 mb-n1  d-none d-md-flex">
      <a class="Header-link " href="https://github.com/" data-hotkey="g d"
  aria-label="Homepage " data-ga-click="Header, go to dashboard, icon:logo">
  <svg class="octicon octicon-mark-github v-align-middle" height="32" viewBox="0 0 16 16" version="1.1" width="32" aria-hidden="true"><path fill-rule="evenodd" d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"></path></svg>
</a>

    </div>

    <div class="Header-item d-md-none">
      <button class="Header-link btn-link js-details-target" type="button" aria-label="Toggle navigation" aria-expanded="false">
        <svg height="24" class="octicon octicon-three-bars" viewBox="0 0 16 16" version="1.1" width="24" aria-hidden="true"><path fill-rule="evenodd" d="M1 2.75A.75.75 0 011.75 2h12.5a.75.75 0 110 1.5H1.75A.75.75 0 011 2.75zm0 5A.75.75 0 011.75 7h12.5a.75.75 0 110 1.5H1.75A.75.75 0 011 7.75zM1.75 12a.75.75 0 100 1.5h12.5a.75.75 0 100-1.5H1.75z"></path></svg>
      </button>
    </div>

    <div class="Header-item Header-item--full flex-column flex-md-row width-full flex-order-2 flex-md-order-none mr-0 mr-md-3 mt-3 mt-md-0 Details-content--hidden-not-important d-md-flex">
          <div class="header-search header-search-current js-header-search-current flex-auto js-site-search position-relative flex-self-stretch flex-md-self-auto mb-3 mb-md-0 mr-0 mr-md-3 scoped-search site-scoped-search js-jump-to js-header-search-current-jump-to"
  role="combobox"
  aria-owns="jump-to-results"
  aria-label="Search or jump to"
  aria-haspopup="listbox"
  aria-expanded="false"
>
  <div class="position-relative">
    <!-- '"` --><!-- </textarea></xmp> --></option></form><form class="js-site-search-form" role="search" aria-label="Site" data-scope-type="Repository" data-scope-id="162637563" data-scoped-search-url="/VolpeUSDOT/FTOT/search" data-unscoped-search-url="/search" action="/VolpeUSDOT/FTOT/search" accept-charset="UTF-8" method="get">
      <label class="form-control input-sm header-search-wrapper p-0 js-chromeless-input-container header-search-wrapper-jump-to position-relative d-flex flex-justify-between flex-items-center">
        <input type="text"
          class="form-control input-sm header-search-input jump-to-field js-jump-to-field js-site-search-focus js-site-search-field is-clearable"
          data-hotkey="s,/"
          name="q"
          value=""
          placeholder="Search or jump to…"
          data-unscoped-placeholder="Search or jump to…"
          data-scoped-placeholder="Search or jump to…"
          autocapitalize="off"
          aria-autocomplete="list"
          aria-controls="jump-to-results"
          aria-label="Search or jump to…"
          data-jump-to-suggestions-path="/_graphql/GetSuggestedNavigationDestinations"
          spellcheck="false"
          autocomplete="off"
          >
          <input type="hidden" value="SBEympUjVwKaGJn2bhs3Ay/Z4jyerb7gkZmjb0v/c9mDkmE2is6RQlhZu44TN4q0apy9nehEiKfCCdB520YMqQ==" data-csrf="true" class="js-data-jump-to-suggestions-path-csrf" />
          <input type="hidden" class="js-site-search-type-field" name="type" >
            <img src="https://github.githubassets.com/images/search-key-slash.svg" alt="" class="mr-2 header-search-key-slash">

            <div class="Box position-absolute overflow-hidden d-none jump-to-suggestions js-jump-to-suggestions-container">
              
<ul class="d-none js-jump-to-suggestions-template-container">
  

<li class="d-flex flex-justify-start flex-items-center p-0 f5 navigation-item js-navigation-item js-jump-to-suggestion" role="option">
  <a tabindex="-1" class="no-underline d-flex flex-auto flex-items-center jump-to-suggestions-path js-jump-to-suggestion-path js-navigation-open p-2" href="">
    <div class="jump-to-octicon js-jump-to-octicon flex-shrink-0 mr-2 text-center d-none">
      <svg height="16" width="16" class="octicon octicon-repo flex-shrink-0 js-jump-to-octicon-repo d-none" title="Repository" aria-label="Repository" viewBox="0 0 16 16" version="1.1" role="img"><path fill-rule="evenodd" d="M2 2.5A2.5 2.5 0 014.5 0h8.75a.75.75 0 01.75.75v12.5a.75.75 0 01-.75.75h-2.5a.75.75 0 110-1.5h1.75v-2h-8a1 1 0 00-.714 1.7.75.75 0 01-1.072 1.05A2.495 2.495 0 012 11.5v-9zm10.5-1V9h-8c-.356 0-.694.074-1 .208V2.5a1 1 0 011-1h8zM5 12.25v3.25a.25.25 0 00.4.2l1.45-1.087a.25.25 0 01.3 0L8.6 15.7a.25.25 0 00.4-.2v-3.25a.25.25 0 00-.25-.25h-3.5a.25.25 0 00-.25.25z"></path></svg>
      <svg height="16" width="16" class="octicon octicon-project flex-shrink-0 js-jump-to-octicon-project d-none" title="Project" aria-label="Project" viewBox="0 0 16 16" version="1.1" role="img"><path fill-rule="evenodd" d="M1.75 0A1.75 1.75 0 000 1.75v12.5C0 15.216.784 16 1.75 16h12.5A1.75 1.75 0 0016 14.25V1.75A1.75 1.75 0 0014.25 0H1.75zM1.5 1.75a.25.25 0 01.25-.25h12.5a.25.25 0 01.25.25v12.5a.25.25 0 01-.25.25H1.75a.25.25 0 01-.25-.25V1.75zM11.75 3a.75.75 0 00-.75.75v7.5a.75.75 0 001.5 0v-7.5a.75.75 0 00-.75-.75zm-8.25.75a.75.75 0 011.5 0v5.5a.75.75 0 01-1.5 0v-5.5zM8 3a.75.75 0 00-.75.75v3.5a.75.75 0 001.5 0v-3.5A.75.75 0 008 3z"></path></svg>
      <svg height="16" width="16" class="octicon octicon-search flex-shrink-0 js-jump-to-octicon-search d-none" title="Search" aria-label="Search" viewBox="0 0 16 16" version="1.1" role="img"><path fill-rule="evenodd" d="M11.5 7a4.499 4.499 0 11-8.998 0A4.499 4.499 0 0111.5 7zm-.82 4.74a6 6 0 111.06-1.06l3.04 3.04a.75.75 0 11-1.06 1.06l-3.04-3.04z"></path></svg>
    </div>

    <img class="avatar mr-2 flex-shrink-0 js-jump-to-suggestion-avatar d-none" alt="" aria-label="Team" src="" width="28" height="28">

    <div class="jump-to-suggestion-name js-jump-to-suggestion-name flex-auto overflow-hidden text-left no-wrap css-truncate css-truncate-target">
    </div>

    <div class="border rounded-1 flex-shrink-0 bg-gray px-1 text-gray-light ml-1 f6 d-none js-jump-to-badge-search">
      <span class="js-jump-to-badge-search-text-default d-none" aria-label="in this repository">
        In this repository
      </span>
      <span class="js-jump-to-badge-search-text-global d-none" aria-label="in all of GitHub">
        All GitHub
      </span>
      <span aria-hidden="true" class="d-inline-block ml-1 v-align-middle">↵</span>
    </div>

    <div aria-hidden="true" class="border rounded-1 flex-shrink-0 bg-gray px-1 text-gray-light ml-1 f6 d-none d-on-nav-focus js-jump-to-badge-jump">
      Jump to
      <span class="d-inline-block ml-1 v-align-middle">↵</span>
    </div>
  </a>
</li>

</ul>

<ul class="d-none js-jump-to-no-results-template-container">
  <li class="d-flex flex-justify-center flex-items-center f5 d-none js-jump-to-suggestion p-2">
    <span class="text-gray">No suggested jump to results</span>
  </li>
</ul>

<ul id="jump-to-results" role="listbox" class="p-0 m-0 js-navigation-container jump-to-suggestions-results-container js-jump-to-suggestions-results-container">
  

<li class="d-flex flex-justify-start flex-items-center p-0 f5 navigation-item js-navigation-item js-jump-to-scoped-search d-none" role="option">
  <a tabindex="-1" class="no-underline d-flex flex-auto flex-items-center jump-to-suggestions-path js-jump-to-suggestion-path js-navigation-open p-2" href="">
    <div class="jump-to-octicon js-jump-to-octicon flex-shrink-0 mr-2 text-center d-none">
      <svg height="16" width="16" class="octicon octicon-repo flex-shrink-0 js-jump-to-octicon-repo d-none" title="Repository" aria-label="Repository" viewBox="0 0 16 16" version="1.1" role="img"><path fill-rule="evenodd" d="M2 2.5A2.5 2.5 0 014.5 0h8.75a.75.75 0 01.75.75v12.5a.75.75 0 01-.75.75h-2.5a.75.75 0 110-1.5h1.75v-2h-8a1 1 0 00-.714 1.7.75.75 0 01-1.072 1.05A2.495 2.495 0 012 11.5v-9zm10.5-1V9h-8c-.356 0-.694.074-1 .208V2.5a1 1 0 011-1h8zM5 12.25v3.25a.25.25 0 00.4.2l1.45-1.087a.25.25 0 01.3 0L8.6 15.7a.25.25 0 00.4-.2v-3.25a.25.25 0 00-.25-.25h-3.5a.25.25 0 00-.25.25z"></path></svg>
      <svg height="16" width="16" class="octicon octicon-project flex-shrink-0 js-jump-to-octicon-project d-none" title="Project" aria-label="Project" viewBox="0 0 16 16" version="1.1" role="img"><path fill-rule="evenodd" d="M1.75 0A1.75 1.75 0 000 1.75v12.5C0 15.216.784 16 1.75 16h12.5A1.75 1.75 0 0016 14.25V1.75A1.75 1.75 0 0014.25 0H1.75zM1.5 1.75a.25.25 0 01.25-.25h12.5a.25.25 0 01.25.25v12.5a.25.25 0 01-.25.25H1.75a.25.25 0 01-.25-.25V1.75zM11.75 3a.75.75 0 00-.75.75v7.5a.75.75 0 001.5 0v-7.5a.75.75 0 00-.75-.75zm-8.25.75a.75.75 0 011.5 0v5.5a.75.75 0 01-1.5 0v-5.5zM8 3a.75.75 0 00-.75.75v3.5a.75.75 0 001.5 0v-3.5A.75.75 0 008 3z"></path></svg>
      <svg height="16" width="16" class="octicon octicon-search flex-shrink-0 js-jump-to-octicon-search d-none" title="Search" aria-label="Search" viewBox="0 0 16 16" version="1.1" role="img"><path fill-rule="evenodd" d="M11.5 7a4.499 4.499 0 11-8.998 0A4.499 4.499 0 0111.5 7zm-.82 4.74a6 6 0 111.06-1.06l3.04 3.04a.75.75 0 11-1.06 1.06l-3.04-3.04z"></path></svg>
    </div>

    <img class="avatar mr-2 flex-shrink-0 js-jump-to-suggestion-avatar d-none" alt="" aria-label="Team" src="" width="28" height="28">

    <div class="jump-to-suggestion-name js-jump-to-suggestion-name flex-auto overflow-hidden text-left no-wrap css-truncate css-truncate-target">
    </div>

    <div class="border rounded-1 flex-shrink-0 bg-gray px-1 text-gray-light ml-1 f6 d-none js-jump-to-badge-search">
      <span class="js-jump-to-badge-search-text-default d-none" aria-label="in this repository">
        In this repository
      </span>
      <span class="js-jump-to-badge-search-text-global d-none" aria-label="in all of GitHub">
        All GitHub
      </span>
      <span aria-hidden="true" class="d-inline-block ml-1 v-align-middle">↵</span>
    </div>

    <div aria-hidden="true" class="border rounded-1 flex-shrink-0 bg-gray px-1 text-gray-light ml-1 f6 d-none d-on-nav-focus js-jump-to-badge-jump">
      Jump to
      <span class="d-inline-block ml-1 v-align-middle">↵</span>
    </div>
  </a>
</li>

  

<li class="d-flex flex-justify-start flex-items-center p-0 f5 navigation-item js-navigation-item js-jump-to-global-search d-none" role="option">
  <a tabindex="-1" class="no-underline d-flex flex-auto flex-items-center jump-to-suggestions-path js-jump-to-suggestion-path js-navigation-open p-2" href="">
    <div class="jump-to-octicon js-jump-to-octicon flex-shrink-0 mr-2 text-center d-none">
      <svg height="16" width="16" class="octicon octicon-repo flex-shrink-0 js-jump-to-octicon-repo d-none" title="Repository" aria-label="Repository" viewBox="0 0 16 16" version="1.1" role="img"><path fill-rule="evenodd" d="M2 2.5A2.5 2.5 0 014.5 0h8.75a.75.75 0 01.75.75v12.5a.75.75 0 01-.75.75h-2.5a.75.75 0 110-1.5h1.75v-2h-8a1 1 0 00-.714 1.7.75.75 0 01-1.072 1.05A2.495 2.495 0 012 11.5v-9zm10.5-1V9h-8c-.356 0-.694.074-1 .208V2.5a1 1 0 011-1h8zM5 12.25v3.25a.25.25 0 00.4.2l1.45-1.087a.25.25 0 01.3 0L8.6 15.7a.25.25 0 00.4-.2v-3.25a.25.25 0 00-.25-.25h-3.5a.25.25 0 00-.25.25z"></path></svg>
      <svg height="16" width="16" class="octicon octicon-project flex-shrink-0 js-jump-to-octicon-project d-none" title="Project" aria-label="Project" viewBox="0 0 16 16" version="1.1" role="img"><path fill-rule="evenodd" d="M1.75 0A1.75 1.75 0 000 1.75v12.5C0 15.216.784 16 1.75 16h12.5A1.75 1.75 0 0016 14.25V1.75A1.75 1.75 0 0014.25 0H1.75zM1.5 1.75a.25.25 0 01.25-.25h12.5a.25.25 0 01.25.25v12.5a.25.25 0 01-.25.25H1.75a.25.25 0 01-.25-.25V1.75zM11.75 3a.75.75 0 00-.75.75v7.5a.75.75 0 001.5 0v-7.5a.75.75 0 00-.75-.75zm-8.25.75a.75.75 0 011.5 0v5.5a.75.75 0 01-1.5 0v-5.5zM8 3a.75.75 0 00-.75.75v3.5a.75.75 0 001.5 0v-3.5A.75.75 0 008 3z"></path></svg>
      <svg height="16" width="16" class="octicon octicon-search flex-shrink-0 js-jump-to-octicon-search d-none" title="Search" aria-label="Search" viewBox="0 0 16 16" version="1.1" role="img"><path fill-rule="evenodd" d="M11.5 7a4.499 4.499 0 11-8.998 0A4.499 4.499 0 0111.5 7zm-.82 4.74a6 6 0 111.06-1.06l3.04 3.04a.75.75 0 11-1.06 1.06l-3.04-3.04z"></path></svg>
    </div>

    <img class="avatar mr-2 flex-shrink-0 js-jump-to-suggestion-avatar d-none" alt="" aria-label="Team" src="" width="28" height="28">

    <div class="jump-to-suggestion-name js-jump-to-suggestion-name flex-auto overflow-hidden text-left no-wrap css-truncate css-truncate-target">
    </div>

    <div class="border rounded-1 flex-shrink-0 bg-gray px-1 text-gray-light ml-1 f6 d-none js-jump-to-badge-search">
      <span class="js-jump-to-badge-search-text-default d-none" aria-label="in this repository">
        In this repository
      </span>
      <span class="js-jump-to-badge-search-text-global d-none" aria-label="in all of GitHub">
        All GitHub
      </span>
      <span aria-hidden="true" class="d-inline-block ml-1 v-align-middle">↵</span>
    </div>

    <div aria-hidden="true" class="border rounded-1 flex-shrink-0 bg-gray px-1 text-gray-light ml-1 f6 d-none d-on-nav-focus js-jump-to-badge-jump">
      Jump to
      <span class="d-inline-block ml-1 v-align-middle">↵</span>
    </div>
  </a>
</li>


    <li class="d-flex flex-justify-center flex-items-center p-0 f5 js-jump-to-suggestion">
      <img src="https://github.githubassets.com/images/spinners/octocat-spinner-128.gif" alt="Octocat Spinner Icon" class="m-2" width="28">
    </li>
</ul>

            </div>
      </label>
</form>  </div>
</div>


      <nav class="d-flex flex-column flex-md-row flex-self-stretch flex-md-self-auto" aria-label="Global">
      <a class="Header-link py-md-3 d-block d-md-none py-2 border-top border-md-top-0 border-white-fade-15" data-ga-click="Header, click, Nav menu - item:dashboard:user" aria-label="Dashboard" href="/dashboard">
        Dashboard
</a>
    <a class="js-selected-navigation-item Header-link mt-md-n3 mb-md-n3 py-2 py-md-3 mr-0 mr-md-3 border-top border-md-top-0 border-white-fade-15" data-hotkey="g p" data-ga-click="Header, click, Nav menu - item:pulls context:user" aria-label="Pull requests you created" data-selected-links="/pulls /pulls/assigned /pulls/mentioned /pulls" href="/pulls">
        Pull<span class="d-inline d-md-none d-lg-inline"> request</span>s
</a>
    <a class="js-selected-navigation-item Header-link mt-md-n3 mb-md-n3 py-2 py-md-3 mr-0 mr-md-3 border-top border-md-top-0 border-white-fade-15" data-hotkey="g i" data-ga-click="Header, click, Nav menu - item:issues context:user" aria-label="Issues you created" data-selected-links="/issues /issues/assigned /issues/mentioned /issues" href="/issues">
      Issues
</a>

      <div class="d-flex position-relative">
        <a class="js-selected-navigation-item Header-link flex-auto mt-md-n3 mb-md-n3 py-2 py-md-3 mr-0 mr-md-3 border-top border-md-top-0 border-white-fade-15" data-ga-click="Header, click, Nav menu - item:marketplace context:user" data-octo-click="marketplace_click" data-octo-dimensions="location:nav_bar" data-selected-links=" /marketplace" href="/marketplace">
          Marketplace
</a>        

      </div>

    <a class="js-selected-navigation-item Header-link mt-md-n3 mb-md-n3 py-2 py-md-3 mr-0 mr-md-3 border-top border-md-top-0 border-white-fade-15" data-ga-click="Header, click, Nav menu - item:explore" data-selected-links="/explore /trending /trending/developers /integrations /integrations/feature/code /integrations/feature/collaborate /integrations/feature/ship showcases showcases_search showcases_landing /explore" href="/explore">
      Explore
</a>


    <a class="Header-link d-block d-md-none mr-0 mr-md-3 py-2 py-md-3 border-top border-md-top-0 border-white-fade-15" href="/matthewpearlson">
      <img class="avatar avatar-user" src="https://avatars1.githubusercontent.com/u/45362680?s=40&amp;v=4" width="20" height="20" alt="@matthewpearlson" />
      matthewpearlson
</a>
    <!-- '"` --><!-- </textarea></xmp> --></option></form><form action="/logout" accept-charset="UTF-8" method="post"><input type="hidden" name="authenticity_token" value="EmG+rYCkXRZ0VscqdY5I2GkiF2OT7yiYa2auawczPt2a84wf3jV1NVCWcjvbKOU/C69RNKcTrW6YsNkGQ0CxXg==" />
      <button type="submit" class="Header-link mr-0 mr-md-3 py-2 py-md-3 border-top border-md-top-0 border-white-fade-15 d-md-none btn-link d-block width-full text-left" data-ga-click="Header, sign out, icon:logout" style="padding-left: 2px;">
        <svg class="octicon octicon-sign-out v-align-middle" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M2 2.75C2 1.784 2.784 1 3.75 1h2.5a.75.75 0 010 1.5h-2.5a.25.25 0 00-.25.25v10.5c0 .138.112.25.25.25h2.5a.75.75 0 010 1.5h-2.5A1.75 1.75 0 012 13.25V2.75zm10.44 4.5H6.75a.75.75 0 000 1.5h5.69l-1.97 1.97a.75.75 0 101.06 1.06l3.25-3.25a.75.75 0 000-1.06l-3.25-3.25a.75.75 0 10-1.06 1.06l1.97 1.97z"></path></svg>
        Sign out
      </button>
</form></nav>

    </div>

    <div class="Header-item Header-item--full flex-justify-center d-md-none position-relative">
      <a class="Header-link " href="https://github.com/" data-hotkey="g d"
  aria-label="Homepage " data-ga-click="Header, go to dashboard, icon:logo">
  <svg class="octicon octicon-mark-github v-align-middle" height="32" viewBox="0 0 16 16" version="1.1" width="32" aria-hidden="true"><path fill-rule="evenodd" d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"></path></svg>
</a>

    </div>

    <div class="Header-item mr-0 mr-md-3 flex-order-1 flex-md-order-none">
      

    <notification-indicator class="js-socket-channel" data-channel="eyJjIjoibm90aWZpY2F0aW9uLWNoYW5nZWQ6NDUzNjI2ODAiLCJ0IjoxNjAyNjEyMDIzfQ==--152bac2b850d52499266b2f781173fe9803593e8596c9046b3b33a8fffa74b79">
      <a href="/notifications"
         class="Header-link notification-indicator position-relative tooltipped tooltipped-sw"
         
         aria-label="You have unread notifications"
         data-hotkey="g n"
         data-ga-click="Header, go to notifications, icon:unread"
         data-target="notification-indicator.link">
         <span class="mail-status unread " data-target="notification-indicator.modifier"></span>
         <svg class="octicon octicon-bell" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path d="M8 16a2 2 0 001.985-1.75c.017-.137-.097-.25-.235-.25h-3.5c-.138 0-.252.113-.235.25A2 2 0 008 16z"></path><path fill-rule="evenodd" d="M8 1.5A3.5 3.5 0 004.5 5v2.947c0 .346-.102.683-.294.97l-1.703 2.556a.018.018 0 00-.003.01l.001.006c0 .002.002.004.004.006a.017.017 0 00.006.004l.007.001h10.964l.007-.001a.016.016 0 00.006-.004.016.016 0 00.004-.006l.001-.007a.017.017 0 00-.003-.01l-1.703-2.554a1.75 1.75 0 01-.294-.97V5A3.5 3.5 0 008 1.5zM3 5a5 5 0 0110 0v2.947c0 .05.015.098.042.139l1.703 2.555A1.518 1.518 0 0113.482 13H2.518a1.518 1.518 0 01-1.263-2.36l1.703-2.554A.25.25 0 003 7.947V5z"></path></svg>
      </a>
    </notification-indicator>

    </div>


    <div class="Header-item position-relative d-none d-md-flex">
      <details class="details-overlay details-reset">
  <summary class="Header-link"
      aria-label="Create new…"
      data-ga-click="Header, create new, icon:add">
      <svg class="octicon octicon-plus" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M8 2a.75.75 0 01.75.75v4.5h4.5a.75.75 0 010 1.5h-4.5v4.5a.75.75 0 01-1.5 0v-4.5h-4.5a.75.75 0 010-1.5h4.5v-4.5A.75.75 0 018 2z"></path></svg> <span class="dropdown-caret"></span>
  </summary>
  <details-menu class="dropdown-menu dropdown-menu-sw">
    
<a role="menuitem" class="dropdown-item" href="/new" data-ga-click="Header, create new repository">
  New repository
</a>

  <a role="menuitem" class="dropdown-item" href="/new/import" data-ga-click="Header, import a repository">
    Import repository
  </a>

<a role="menuitem" class="dropdown-item" href="https://gist.github.com/" data-ga-click="Header, create new gist">
  New gist
</a>

  <a role="menuitem" class="dropdown-item" href="/organizations/new" data-ga-click="Header, create new organization">
    New organization
  </a>


  <div role="none" class="dropdown-divider"></div>
  <div class="dropdown-header">
    <span title="VolpeUSDOT/FTOT">This repository</span>
  </div>
    <a role="menuitem" class="dropdown-item" href="/VolpeUSDOT/FTOT/issues/new/choose" data-ga-click="Header, create new issue" data-skip-pjax>
      New issue
    </a>


  </details-menu>
</details>

    </div>

    <div class="Header-item position-relative mr-0 d-none d-md-flex">
      
  <details class="details-overlay details-reset js-feature-preview-indicator-container" data-feature-preview-indicator-src="/users/matthewpearlson/feature_preview/indicator_check">

  <summary class="Header-link"
    aria-label="View profile and more"
    data-ga-click="Header, show menu, icon:avatar">
    <img
  alt="@matthewpearlson"
  width="20"
  height="20"
  src="https://avatars2.githubusercontent.com/u/45362680?s=60&amp;v=4"
  class="avatar avatar-user " />

      <span class="feature-preview-indicator js-feature-preview-indicator" style="top: 10px;" hidden></span>
    <span class="dropdown-caret"></span>
  </summary>
  <details-menu class="dropdown-menu dropdown-menu-sw" style="width: 180px" >
    <div class="header-nav-current-user css-truncate"><a role="menuitem" class="no-underline user-profile-link px-3 pt-2 pb-2 mb-n2 mt-n1 d-block" href="/matthewpearlson" data-ga-click="Header, go to profile, text:Signed in as">Signed in as <strong class="css-truncate-target">matthewpearlson</strong></a></div>
    <div role="none" class="dropdown-divider"></div>

      <div class="pl-3 pr-3 f6 user-status-container js-user-status-context lh-condensed" data-url="/users/status?compact=1&amp;link_mentions=0&amp;truncate=1">
        
<div class="js-user-status-container rounded-1 px-2 py-1 mt-2 border"
  data-team-hovercards-enabled>
  <details class="js-user-status-details details-reset details-overlay details-overlay-dark">
    <summary class="btn-link btn-block link-gray no-underline js-toggle-user-status-edit toggle-user-status-edit "
      role="menuitem" data-hydro-click="{&quot;event_type&quot;:&quot;user_profile.click&quot;,&quot;payload&quot;:{&quot;profile_user_id&quot;:29147435,&quot;target&quot;:&quot;EDIT_USER_STATUS&quot;,&quot;user_id&quot;:45362680,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;}}" data-hydro-click-hmac="ffbb7c453652548cb6066fc029eacf465844485aaeddc14931cda5da9f449140">
      <div class="d-flex flex-items-center flex-items-stretch">
        <div class="f6 lh-condensed user-status-header d-flex user-status-emoji-only-header circle">
          <div class="user-status-emoji-container flex-shrink-0 mr-2 d-flex flex-items-center flex-justify-center lh-condensed-ultra v-align-bottom">
            <div><g-emoji class="g-emoji" alias="dart" fallback-src="https://github.githubassets.com/images/icons/emoji/unicode/1f3af.png">🎯</g-emoji></div>
          </div>
        </div>
        <div class="
          
           user-status-message-wrapper f6 min-width-0"
           style="line-height: 20px;" >
          <div class="css-truncate css-truncate-target width-fit text-gray-dark text-left">
                <span>Focusing</span>
          </div>
        </div>
      </div>
    </summary>
    <details-dialog class="details-dialog rounded-1 anim-fade-in fast Box Box--overlay" role="dialog" tabindex="-1">
      <!-- '"` --><!-- </textarea></xmp> --></option></form><form class="position-relative flex-auto js-user-status-form" action="/users/status?circle=0&amp;compact=1&amp;link_mentions=0&amp;truncate=1" accept-charset="UTF-8" method="post"><input type="hidden" name="_method" value="put" /><input type="hidden" name="authenticity_token" value="d8rA8trET05HkU8CJrmoAm8Z0N6jvU6m/Q4+xjosCg4nFcjfGf0K/DdVD8leXmb74HdgYtnx2mgsleGBx4si3w==" />
        <div class="Box-header bg-gray border-bottom p-3">
          <button class="Box-btn-octicon js-toggle-user-status-edit btn-octicon float-right" type="reset" aria-label="Close dialog" data-close-dialog>
            <svg class="octicon octicon-x" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M3.72 3.72a.75.75 0 011.06 0L8 6.94l3.22-3.22a.75.75 0 111.06 1.06L9.06 8l3.22 3.22a.75.75 0 11-1.06 1.06L8 9.06l-3.22 3.22a.75.75 0 01-1.06-1.06L6.94 8 3.72 4.78a.75.75 0 010-1.06z"></path></svg>
          </button>
          <h3 class="Box-title f5 text-bold text-gray-dark">Edit status</h3>
        </div>
        <input type="hidden" name="emoji" class="js-user-status-emoji-field" value=":dart:">
        <input type="hidden" name="organization_id" class="js-user-status-org-id-field" value="">
        <div class="px-3 py-2 text-gray-dark">
          <div class="js-characters-remaining-container position-relative mt-2">
            <div class="input-group d-table form-group my-0 js-user-status-form-group">
              <span class="input-group-button d-table-cell v-align-middle" style="width: 1%">
                <button type="button" aria-label="Choose an emoji" class="btn-outline btn js-toggle-user-status-emoji-picker btn-open-emoji-picker p-0">
                  <span class="js-user-status-original-emoji" hidden><div><g-emoji class="g-emoji" alias="dart" fallback-src="https://github.githubassets.com/images/icons/emoji/unicode/1f3af.png">🎯</g-emoji></div></span>
                  <span class="js-user-status-custom-emoji"><div><g-emoji class="g-emoji" alias="dart" fallback-src="https://github.githubassets.com/images/icons/emoji/unicode/1f3af.png">🎯</g-emoji></div></span>
                  <span class="js-user-status-no-emoji-icon" hidden>
                    <svg class="octicon octicon-smiley" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M1.5 8a6.5 6.5 0 1113 0 6.5 6.5 0 01-13 0zM8 0a8 8 0 100 16A8 8 0 008 0zM5 8a1 1 0 100-2 1 1 0 000 2zm7-1a1 1 0 11-2 0 1 1 0 012 0zM5.32 9.636a.75.75 0 011.038.175l.007.009c.103.118.22.222.35.31.264.178.683.37 1.285.37.602 0 1.02-.192 1.285-.371.13-.088.247-.192.35-.31l.007-.008a.75.75 0 111.222.87l-.614-.431c.614.43.614.431.613.431v.001l-.001.002-.002.003-.005.007-.014.019a1.984 1.984 0 01-.184.213c-.16.166-.338.316-.53.445-.63.418-1.37.638-2.127.629-.946 0-1.652-.308-2.126-.63a3.32 3.32 0 01-.715-.657l-.014-.02-.005-.006-.002-.003v-.002h-.001l.613-.432-.614.43a.75.75 0 01.183-1.044h.001z"></path></svg>
                  </span>
                </button>
              </span>
              <text-expander keys=": @" data-mention-url="/autocomplete/user-suggestions" data-emoji-url="/autocomplete/emoji">
                <input
                  type="text"
                  autocomplete="off"
                  data-no-org-url="/autocomplete/user-suggestions"
                  data-org-url="/suggestions?mention_suggester=1"
                  data-maxlength="80"
                  class="d-table-cell width-full form-control js-user-status-message-field js-characters-remaining-field"
                  placeholder="What's happening?"
                  name="message"
                  value="Focusing"
                  aria-label="What is your current status?">
              </text-expander>
              <div class="error">Could not update your status, please try again.</div>
            </div>
            <div style="margin-left: 53px" class="my-1 text-small label-characters-remaining js-characters-remaining" data-suffix="remaining" hidden>
              80 remaining
            </div>
          </div>
          <include-fragment class="js-user-status-emoji-picker" data-url="/users/status/emoji"></include-fragment>
          <div class="overflow-auto ml-n3 mr-n3 px-3 border-bottom" style="max-height: 33vh">
            <div class="user-status-suggestions js-user-status-suggestions collapsed overflow-hidden">
              <h4 class="f6 text-normal my-3">Suggestions:</h4>
              <div class="mx-3 mt-2 clearfix">
                  <div class="float-left col-6">
                      <button type="button" value=":palm_tree:" class="d-flex flex-items-baseline flex-items-stretch lh-condensed f6 btn-link link-gray no-underline js-predefined-user-status mb-1">
                        <div class="emoji-status-width mr-2 v-align-middle js-predefined-user-status-emoji">
                          <g-emoji alias="palm_tree" fallback-src="https://github.githubassets.com/images/icons/emoji/unicode/1f334.png">🌴</g-emoji>
                        </div>
                        <div class="d-flex flex-items-center no-underline js-predefined-user-status-message ws-normal text-left" style="border-left: 1px solid transparent">
                          On vacation
                        </div>
                      </button>
                      <button type="button" value=":face_with_thermometer:" class="d-flex flex-items-baseline flex-items-stretch lh-condensed f6 btn-link link-gray no-underline js-predefined-user-status mb-1">
                        <div class="emoji-status-width mr-2 v-align-middle js-predefined-user-status-emoji">
                          <g-emoji alias="face_with_thermometer" fallback-src="https://github.githubassets.com/images/icons/emoji/unicode/1f912.png">🤒</g-emoji>
                        </div>
                        <div class="d-flex flex-items-center no-underline js-predefined-user-status-message ws-normal text-left" style="border-left: 1px solid transparent">
                          Out sick
                        </div>
                      </button>
                  </div>
                  <div class="float-left col-6">
                      <button type="button" value=":house:" class="d-flex flex-items-baseline flex-items-stretch lh-condensed f6 btn-link link-gray no-underline js-predefined-user-status mb-1">
                        <div class="emoji-status-width mr-2 v-align-middle js-predefined-user-status-emoji">
                          <g-emoji alias="house" fallback-src="https://github.githubassets.com/images/icons/emoji/unicode/1f3e0.png">🏠</g-emoji>
                        </div>
                        <div class="d-flex flex-items-center no-underline js-predefined-user-status-message ws-normal text-left" style="border-left: 1px solid transparent">
                          Working from home
                        </div>
                      </button>
                      <button type="button" value=":dart:" class="d-flex flex-items-baseline flex-items-stretch lh-condensed f6 btn-link link-gray no-underline js-predefined-user-status mb-1">
                        <div class="emoji-status-width mr-2 v-align-middle js-predefined-user-status-emoji">
                          <g-emoji alias="dart" fallback-src="https://github.githubassets.com/images/icons/emoji/unicode/1f3af.png">🎯</g-emoji>
                        </div>
                        <div class="d-flex flex-items-center no-underline js-predefined-user-status-message ws-normal text-left" style="border-left: 1px solid transparent">
                          Focusing
                        </div>
                      </button>
                  </div>
              </div>
            </div>
            <div class="user-status-limited-availability-container">
              <div class="form-checkbox my-0">
                <input type="checkbox" name="limited_availability" value="1" class="js-user-status-limited-availability-checkbox" data-default-message="I may be slow to respond." aria-describedby="limited-availability-help-text-truncate-true-compact-true" id="limited-availability-truncate-true-compact-true">
                <label class="d-block f5 text-gray-dark mb-1" for="limited-availability-truncate-true-compact-true">
                  Busy
                </label>
                <p class="note" id="limited-availability-help-text-truncate-true-compact-true">
                  When others mention you, assign you, or request your review,
                  GitHub will let them know that you have limited availability.
                </p>
              </div>
            </div>
          </div>
          <div class="d-inline-block f5 mr-2 pt-3 pb-2" >
  <div class="d-inline-block mr-1">
    Clear status
  </div>

  <details class="js-user-status-expire-drop-down f6 dropdown details-reset details-overlay d-inline-block mr-2">
    <summary class="btn btn-sm v-align-baseline" aria-haspopup="true">
      <div class="js-user-status-expiration-interval-selected d-inline-block v-align-baseline">
        Never
      </div>
      <div class="dropdown-caret"></div>
    </summary>

    <ul class="dropdown-menu dropdown-menu-se pl-0 overflow-auto" style="width: 220px; max-height: 15.5em">
      <li>
        <button type="button" class="btn-link dropdown-item js-user-status-expire-button ws-normal" title="Never">
          <span class="d-inline-block text-bold mb-1">Never</span>
          <div class="f6 lh-condensed">Keep this status until you clear your status or edit your status.</div>
        </button>
      </li>
      <li class="dropdown-divider" role="none"></li>
        <li>
          <button type="button" class="btn-link dropdown-item ws-normal js-user-status-expire-button" title="in 30 minutes" value="2020-10-13T13:30:23-05:00">
            in 30 minutes
          </button>
        </li>
        <li>
          <button type="button" class="btn-link dropdown-item ws-normal js-user-status-expire-button" title="in 1 hour" value="2020-10-13T14:00:23-05:00">
            in 1 hour
          </button>
        </li>
        <li>
          <button type="button" class="btn-link dropdown-item ws-normal js-user-status-expire-button" title="in 4 hours" value="2020-10-13T17:00:23-05:00">
            in 4 hours
          </button>
        </li>
        <li>
          <button type="button" class="btn-link dropdown-item ws-normal js-user-status-expire-button" title="today" value="2020-10-13T23:59:59-05:00">
            today
          </button>
        </li>
        <li>
          <button type="button" class="btn-link dropdown-item ws-normal js-user-status-expire-button" title="this week" value="2020-10-18T23:59:59-05:00">
            this week
          </button>
        </li>
    </ul>
  </details>
  <input class="js-user-status-expiration-date-input" type="hidden" name="expires_at" value="">
</div>

          <include-fragment class="js-user-status-org-picker" data-url="/users/status/organizations"></include-fragment>
        </div>
        <div class="d-flex flex-items-center flex-justify-between p-3 border-top">
          <button type="submit"  class="width-full btn btn-primary mr-2 js-user-status-submit">
            Set status
          </button>
          <button type="button"  class="width-full js-clear-user-status-button btn ml-2 js-user-status-exists">
            Clear status
          </button>
        </div>
</form>    </details-dialog>
  </details>
</div>

      </div>
      <div role="none" class="dropdown-divider"></div>

    <a role="menuitem" class="dropdown-item" href="/matthewpearlson" data-ga-click="Header, go to profile, text:your profile" data-hydro-click="{&quot;event_type&quot;:&quot;global_header.user_menu_dropdown.click&quot;,&quot;payload&quot;:{&quot;request_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;target&quot;:&quot;YOUR_PROFILE&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="d447786be6077205f6a39457b51465c233dcaa8cbe522332264aa63bad029aa6" >Your profile</a>

    <a role="menuitem" class="dropdown-item" href="/matthewpearlson?tab=repositories" data-ga-click="Header, go to repositories, text:your repositories" data-hydro-click="{&quot;event_type&quot;:&quot;global_header.user_menu_dropdown.click&quot;,&quot;payload&quot;:{&quot;request_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;target&quot;:&quot;YOUR_REPOSITORIES&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="a78c7343e22dede910ce49153a04f0b9800600815011cab0754a0d0d6128a479" >Your repositories</a>

      <a role="menuitem"
         class="dropdown-item"
         href="/settings/organizations"
         
         data-ga-click="Header, go to organizations, text:your organizations"
         data-hydro-click="{&quot;event_type&quot;:&quot;global_header.user_menu_dropdown.click&quot;,&quot;payload&quot;:{&quot;request_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;target&quot;:&quot;YOUR_ORGANIZATIONS&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="23414a46fa222ea501449100098b494220e2465c8ed49a0c77f9b62ec733a8fa"
         >Your organizations</a>

      <a role="menuitem"
        class="dropdown-item"
        href="/settings/enterprises"
        
        data-ga-click="Header, go to enterprises, text:your enterprises"
        data-hydro-click="{&quot;event_type&quot;:&quot;global_header.user_menu_dropdown.click&quot;,&quot;payload&quot;:{&quot;request_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;target&quot;:&quot;YOUR_ENTERPRISES&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="e116d803a4172100f6a18a0602f20d5137ded154aefcf10d4a114ba750363cf6"
        >Your enterprises</a>

    <a role="menuitem" class="dropdown-item" href="/matthewpearlson?tab=projects" data-ga-click="Header, go to projects, text:your projects" data-hydro-click="{&quot;event_type&quot;:&quot;global_header.user_menu_dropdown.click&quot;,&quot;payload&quot;:{&quot;request_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;target&quot;:&quot;YOUR_PROJECTS&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="0614a640456ac3c6dd744c37680573b1f9c0fad22f41cac7f6d11ddffdd4cf7a" >Your projects</a>


    <a role="menuitem" class="dropdown-item" href="/matthewpearlson?tab=stars" data-ga-click="Header, go to starred repos, text:your stars" data-hydro-click="{&quot;event_type&quot;:&quot;global_header.user_menu_dropdown.click&quot;,&quot;payload&quot;:{&quot;request_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;target&quot;:&quot;YOUR_STARS&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="f3bc142da05326571e8dc2a66e8a1f60e1cf0575352e38412235c3a4ec3d0a87" >Your stars</a>
      <a role="menuitem" class="dropdown-item" href="https://gist.github.com/mine" data-ga-click="Header, your gists, text:your gists" data-hydro-click="{&quot;event_type&quot;:&quot;global_header.user_menu_dropdown.click&quot;,&quot;payload&quot;:{&quot;request_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;target&quot;:&quot;YOUR_GISTS&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="5deae2ddad2a98dbc498403304abaec5d426a716904d631464e50343ea5c0a99" >Your gists</a>





    <div role="none" class="dropdown-divider"></div>
      
<div id="feature-enrollment-toggle" class="hide-sm hide-md feature-preview-details position-relative">
  <button
    type="button"
    class="dropdown-item btn-link"
    role="menuitem"
    data-feature-preview-trigger-url="/users/matthewpearlson/feature_previews"
    data-feature-preview-close-details="{&quot;event_type&quot;:&quot;feature_preview.clicks.close_modal&quot;,&quot;payload&quot;:{&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}"
    data-feature-preview-close-hmac="085a4660957a0515dc3fc745b4c74b12d58b127b571052bb6cab203046603ebb"
    data-hydro-click="{&quot;event_type&quot;:&quot;feature_preview.clicks.open_modal&quot;,&quot;payload&quot;:{&quot;link_location&quot;:&quot;user_dropdown&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}"
    data-hydro-click-hmac="be0600c30b2c2d6520559c511f09a442352fc5a8a7283c1f5d8e89eae49ca1c6"
  >
    Feature preview
  </button>
    <span class="feature-preview-indicator js-feature-preview-indicator" hidden></span>
</div>

    <a role="menuitem" class="dropdown-item" href="https://docs.github.com" data-ga-click="Header, go to help, text:help" data-hydro-click="{&quot;event_type&quot;:&quot;global_header.user_menu_dropdown.click&quot;,&quot;payload&quot;:{&quot;request_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;target&quot;:&quot;HELP&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="e22a3201b4013f7976416ee182256e9cc9391e6cce21cd816083b6b096ce0be4" >Help</a>
    <a role="menuitem" class="dropdown-item" href="/settings/profile" data-ga-click="Header, go to settings, icon:settings" data-hydro-click="{&quot;event_type&quot;:&quot;global_header.user_menu_dropdown.click&quot;,&quot;payload&quot;:{&quot;request_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;target&quot;:&quot;SETTINGS&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="83a168ee9dc53cfdc78a4de4b2d9cdd220664999ea5564cbef976a82a56e6795" >Settings</a>
    <!-- '"` --><!-- </textarea></xmp> --></option></form><form class="logout-form" action="/logout" accept-charset="UTF-8" method="post"><input type="hidden" name="authenticity_token" value="q5SrAo4EcOX7KsAu5mRJ9tlNFiMkAjeUcpQp/vpZYu4jBpmw0JVYxt/qdT9IwuQRu8BQdBD+smKBQl6TvirtbQ==" />
      
      <button type="submit" class="dropdown-item dropdown-signout" data-ga-click="Header, sign out, icon:logout" data-hydro-click="{&quot;event_type&quot;:&quot;global_header.user_menu_dropdown.click&quot;,&quot;payload&quot;:{&quot;request_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;target&quot;:&quot;SIGN_OUT&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="4dbdba5153c1109b39a484a2bdcc00ae5df12b5a617314322dba1ffc0894c33f"  role="menuitem">
        Sign out
      </button>
      <input type="text" name="required_field_97af" hidden="hidden" class="form-control" /><input type="hidden" name="timestamp" value="1602612023864" class="form-control" /><input type="hidden" name="timestamp_secret" value="2a0ff75b91e7644e12582c84e80856bfd5704f927054276321bf17ae12c130a4" class="form-control" />
</form>  </details-menu>
</details>

    </div>
</header>

          

    </div>

  <div id="start-of-content" class="show-on-focus"></div>





    <div data-pjax-replace id="js-flash-container">


  <template class="js-flash-template">
    <div class="flash flash-full  {{ className }}">
  <div class=" px-2" >
    <button class="flash-close js-flash-close" type="button" aria-label="Dismiss this message">
      <svg class="octicon octicon-x" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M3.72 3.72a.75.75 0 011.06 0L8 6.94l3.22-3.22a.75.75 0 111.06 1.06L9.06 8l3.22 3.22a.75.75 0 11-1.06 1.06L8 9.06l-3.22 3.22a.75.75 0 01-1.06-1.06L6.94 8 3.72 4.78a.75.75 0 010-1.06z"></path></svg>
    </button>
    
      <div>{{ message }}</div>

  </div>
</div>
  </template>
</div>


  

  <include-fragment class="js-notification-shelf-include-fragment" data-base-src="https://github.com/notifications/beta/shelf"></include-fragment>



  <div
    class="application-main "
    data-commit-hovercards-enabled
    data-discussion-hovercards-enabled
    data-issue-and-pr-hovercards-enabled
  >
        <div itemscope itemtype="http://schema.org/SoftwareSourceCode" class="">
    <main  >
      

    


    






  


  <div class="bg-gray-light pt-3 hide-full-screen mb-5">

      <div class="d-flex mb-3 px-3 px-md-4 px-lg-5">

        <div class="flex-auto min-width-0 width-fit mr-3">
            <h1 class=" d-flex flex-wrap flex-items-center break-word f3 text-normal">
    <svg class="octicon octicon-lock text-gray mr-2" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M4 4v2h-.25A1.75 1.75 0 002 7.75v5.5c0 .966.784 1.75 1.75 1.75h8.5A1.75 1.75 0 0014 13.25v-5.5A1.75 1.75 0 0012.25 6H12V4a4 4 0 10-8 0zm6.5 2V4a2.5 2.5 0 00-5 0v2h5zM12 7.5h.25a.25.25 0 01.25.25v5.5a.25.25 0 01-.25.25h-8.5a.25.25 0 01-.25-.25v-5.5a.25.25 0 01.25-.25H12z"></path></svg>
    <span class="author flex-self-stretch" itemprop="author">
      <a class="url fn" rel="author" data-hovercard-type="organization" data-hovercard-url="/orgs/VolpeUSDOT/hovercard" href="/VolpeUSDOT">VolpeUSDOT</a>
    </span>
    <span class="mx-1 flex-self-stretch">/</span>
  <strong itemprop="name" class="mr-2 flex-self-stretch">
    <a data-pjax="#js-repo-pjax-container" class="" href="/VolpeUSDOT/FTOT">FTOT</a>
  </strong>
  <span class="Label Label--outline v-align-middle ">Private</span>
</h1>


        </div>

          <ul class="pagehead-actions flex-shrink-0 d-none d-md-inline" style="padding: 2px 0;">

  <li>
            <form data-remote="true" class="d-flex js-social-form js-social-container" action="/notifications/subscribe" accept-charset="UTF-8" method="post"><input type="hidden" name="authenticity_token" value="lcJs8guljc+9GZ8ag8B6s7kVAeSoTjpXWvdd1fq06vhXpgZfVckFHxF6DUj9H8FMb95aLL53KNDt1MCCWQ5xvQ==" />      <input type="hidden" name="repository_id" value="162637563">

      <details class="details-reset details-overlay select-menu hx_rsm">
        <summary class="btn btn-sm btn-with-count" data-hydro-click="{&quot;event_type&quot;:&quot;repository.click&quot;,&quot;payload&quot;:{&quot;target&quot;:&quot;WATCH_BUTTON&quot;,&quot;repository_id&quot;:162637563,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="342e0d975a49d4a033bd1866174c92a3593213d592bce1718425cf6bf3b10f02" data-ga-click="Repository, click Watch settings, action:blob#show">          <span data-menu-button>
              <svg height="16" class="octicon octicon-eye" viewBox="0 0 16 16" version="1.1" width="16" aria-hidden="true"><path fill-rule="evenodd" d="M1.679 7.932c.412-.621 1.242-1.75 2.366-2.717C5.175 4.242 6.527 3.5 8 3.5c1.473 0 2.824.742 3.955 1.715 1.124.967 1.954 2.096 2.366 2.717a.119.119 0 010 .136c-.412.621-1.242 1.75-2.366 2.717C10.825 11.758 9.473 12.5 8 12.5c-1.473 0-2.824-.742-3.955-1.715C2.92 9.818 2.09 8.69 1.679 8.068a.119.119 0 010-.136zM8 2c-1.981 0-3.67.992-4.933 2.078C1.797 5.169.88 6.423.43 7.1a1.619 1.619 0 000 1.798c.45.678 1.367 1.932 2.637 3.024C4.329 13.008 6.019 14 8 14c1.981 0 3.67-.992 4.933-2.078 1.27-1.091 2.187-2.345 2.637-3.023a1.619 1.619 0 000-1.798c-.45-.678-1.367-1.932-2.637-3.023C11.671 2.992 9.981 2 8 2zm0 8a2 2 0 100-4 2 2 0 000 4z"></path></svg>
              Unwatch
          </span>
          <span class="dropdown-caret"></span>
</summary>        <details-menu
          class="select-menu-modal position-absolute mt-5"
          style="z-index: 99;">
          <div class="select-menu-header">
            <span class="select-menu-title">Notifications</span>
          </div>
          <div class="select-menu-list">
            <button type="submit" name="do" value="included" class="select-menu-item width-full" aria-checked="false" role="menuitemradio">
              <svg classes="select-menu-item-icon" height="16" class="octicon octicon-check select-menu-item-icon" viewBox="0 0 16 16" version="1.1" width="16" aria-hidden="true"><path fill-rule="evenodd" d="M13.78 4.22a.75.75 0 010 1.06l-7.25 7.25a.75.75 0 01-1.06 0L2.22 9.28a.75.75 0 011.06-1.06L6 10.94l6.72-6.72a.75.75 0 011.06 0z"></path></svg>
              <div class="select-menu-item-text">
                <span class="select-menu-item-heading">Not watching</span>
                <span class="description">Be notified only when participating or @mentioned.</span>
                <span class="hidden-select-button-text" data-menu-button-contents>
                  <svg height="16" class="octicon octicon-eye" viewBox="0 0 16 16" version="1.1" width="16" aria-hidden="true"><path fill-rule="evenodd" d="M1.679 7.932c.412-.621 1.242-1.75 2.366-2.717C5.175 4.242 6.527 3.5 8 3.5c1.473 0 2.824.742 3.955 1.715 1.124.967 1.954 2.096 2.366 2.717a.119.119 0 010 .136c-.412.621-1.242 1.75-2.366 2.717C10.825 11.758 9.473 12.5 8 12.5c-1.473 0-2.824-.742-3.955-1.715C2.92 9.818 2.09 8.69 1.679 8.068a.119.119 0 010-.136zM8 2c-1.981 0-3.67.992-4.933 2.078C1.797 5.169.88 6.423.43 7.1a1.619 1.619 0 000 1.798c.45.678 1.367 1.932 2.637 3.024C4.329 13.008 6.019 14 8 14c1.981 0 3.67-.992 4.933-2.078 1.27-1.091 2.187-2.345 2.637-3.023a1.619 1.619 0 000-1.798c-.45-.678-1.367-1.932-2.637-3.023C11.671 2.992 9.981 2 8 2zm0 8a2 2 0 100-4 2 2 0 000 4z"></path></svg>
                  Watch
                </span>
              </div>
            </button>

            <button type="submit" name="do" value="release_only" class="select-menu-item width-full" aria-checked="false" role="menuitemradio">
              <svg classes="select-menu-item-icon" height="16" class="octicon octicon-check select-menu-item-icon" viewBox="0 0 16 16" version="1.1" width="16" aria-hidden="true"><path fill-rule="evenodd" d="M13.78 4.22a.75.75 0 010 1.06l-7.25 7.25a.75.75 0 01-1.06 0L2.22 9.28a.75.75 0 011.06-1.06L6 10.94l6.72-6.72a.75.75 0 011.06 0z"></path></svg>
              <div class="select-menu-item-text">
                <span class="select-menu-item-heading">Releases only</span>
                <span class="description">Be notified of new releases, and when participating or @mentioned.</span>
                <span class="hidden-select-button-text" data-menu-button-contents>
                  <svg height="16" class="octicon octicon-eye" viewBox="0 0 16 16" version="1.1" width="16" aria-hidden="true"><path fill-rule="evenodd" d="M1.679 7.932c.412-.621 1.242-1.75 2.366-2.717C5.175 4.242 6.527 3.5 8 3.5c1.473 0 2.824.742 3.955 1.715 1.124.967 1.954 2.096 2.366 2.717a.119.119 0 010 .136c-.412.621-1.242 1.75-2.366 2.717C10.825 11.758 9.473 12.5 8 12.5c-1.473 0-2.824-.742-3.955-1.715C2.92 9.818 2.09 8.69 1.679 8.068a.119.119 0 010-.136zM8 2c-1.981 0-3.67.992-4.933 2.078C1.797 5.169.88 6.423.43 7.1a1.619 1.619 0 000 1.798c.45.678 1.367 1.932 2.637 3.024C4.329 13.008 6.019 14 8 14c1.981 0 3.67-.992 4.933-2.078 1.27-1.091 2.187-2.345 2.637-3.023a1.619 1.619 0 000-1.798c-.45-.678-1.367-1.932-2.637-3.023C11.671 2.992 9.981 2 8 2zm0 8a2 2 0 100-4 2 2 0 000 4z"></path></svg>
                  Unwatch releases
                </span>
              </div>
            </button>

            <button type="submit" name="do" value="subscribed" class="select-menu-item width-full" aria-checked="true" role="menuitemradio">
              <svg classes="select-menu-item-icon" height="16" class="octicon octicon-check select-menu-item-icon" viewBox="0 0 16 16" version="1.1" width="16" aria-hidden="true"><path fill-rule="evenodd" d="M13.78 4.22a.75.75 0 010 1.06l-7.25 7.25a.75.75 0 01-1.06 0L2.22 9.28a.75.75 0 011.06-1.06L6 10.94l6.72-6.72a.75.75 0 011.06 0z"></path></svg>
              <div class="select-menu-item-text">
                <span class="select-menu-item-heading">Watching</span>
                <span class="description">Be notified of all conversations.</span>
                <span class="hidden-select-button-text" data-menu-button-contents>
                  <svg vertical_align="text-bottom" height="16" class="octicon octicon-eye v-align-text-bottom" viewBox="0 0 16 16" version="1.1" width="16" aria-hidden="true"><path fill-rule="evenodd" d="M1.679 7.932c.412-.621 1.242-1.75 2.366-2.717C5.175 4.242 6.527 3.5 8 3.5c1.473 0 2.824.742 3.955 1.715 1.124.967 1.954 2.096 2.366 2.717a.119.119 0 010 .136c-.412.621-1.242 1.75-2.366 2.717C10.825 11.758 9.473 12.5 8 12.5c-1.473 0-2.824-.742-3.955-1.715C2.92 9.818 2.09 8.69 1.679 8.068a.119.119 0 010-.136zM8 2c-1.981 0-3.67.992-4.933 2.078C1.797 5.169.88 6.423.43 7.1a1.619 1.619 0 000 1.798c.45.678 1.367 1.932 2.637 3.024C4.329 13.008 6.019 14 8 14c1.981 0 3.67-.992 4.933-2.078 1.27-1.091 2.187-2.345 2.637-3.023a1.619 1.619 0 000-1.798c-.45-.678-1.367-1.932-2.637-3.023C11.671 2.992 9.981 2 8 2zm0 8a2 2 0 100-4 2 2 0 000 4z"></path></svg>
                  Unwatch
                </span>
              </div>
            </button>

            <button type="submit" name="do" value="ignore" class="select-menu-item width-full" aria-checked="false" role="menuitemradio">
              <svg classes="select-menu-item-icon" height="16" class="octicon octicon-check select-menu-item-icon" viewBox="0 0 16 16" version="1.1" width="16" aria-hidden="true"><path fill-rule="evenodd" d="M13.78 4.22a.75.75 0 010 1.06l-7.25 7.25a.75.75 0 01-1.06 0L2.22 9.28a.75.75 0 011.06-1.06L6 10.94l6.72-6.72a.75.75 0 011.06 0z"></path></svg>
              <div class="select-menu-item-text">
                <span class="select-menu-item-heading">Ignoring</span>
                <span class="description">Never be notified.</span>
                <span class="hidden-select-button-text" data-menu-button-contents>
                  <svg height="16" class="octicon octicon-bell-slash" viewBox="0 0 16 16" version="1.1" width="16" aria-hidden="true"><path fill-rule="evenodd" d="M8 1.5c-.997 0-1.895.416-2.534 1.086A.75.75 0 014.38 1.55 5 5 0 0113 5v2.373a.75.75 0 01-1.5 0V5A3.5 3.5 0 008 1.5zM4.182 4.31L1.19 2.143a.75.75 0 10-.88 1.214L3 5.305v2.642a.25.25 0 01-.042.139L1.255 10.64A1.518 1.518 0 002.518 13h11.108l1.184.857a.75.75 0 10.88-1.214l-1.375-.996a1.196 1.196 0 00-.013-.01L4.198 4.321a.733.733 0 00-.016-.011zm7.373 7.19L4.5 6.391v1.556c0 .346-.102.683-.294.97l-1.703 2.556a.018.018 0 00-.003.01.015.015 0 00.005.012.017.017 0 00.006.004l.007.001h9.037zM8 16a2 2 0 001.985-1.75c.017-.137-.097-.25-.235-.25h-3.5c-.138 0-.252.113-.235.25A2 2 0 008 16z"></path></svg>
                  Stop ignoring
                </span>
              </div>
            </button>
          </div>
        </details-menu>
      </details>
        <a class="social-count js-social-count"
          href="/VolpeUSDOT/FTOT/watchers"
          aria-label="3 users are watching this repository">
          3
        </a>
</form>
  </li>

  <li>
        <div class="js-toggler-container js-social-container starring-container ">
    <form class="starred js-social-form" action="/VolpeUSDOT/FTOT/unstar" accept-charset="UTF-8" method="post"><input type="hidden" name="authenticity_token" value="vVXD9a0D90XoS2OAfJLuT6fSHMdAX/6Rt9BYFlaRvCEvcu6/ndtZToxpnCgodpx46R1yN4M0Le1Qq2Dc5kY1Ug==" />
      <input type="hidden" name="context" value="repository"></input>
      <button type="submit" class="btn btn-sm btn-with-count  js-toggler-target" aria-label="Unstar this repository" title="Unstar VolpeUSDOT/FTOT" data-hydro-click="{&quot;event_type&quot;:&quot;repository.click&quot;,&quot;payload&quot;:{&quot;target&quot;:&quot;UNSTAR_BUTTON&quot;,&quot;repository_id&quot;:162637563,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="67619659c7a99c3f9e99cc5257bfb5abc6c2e2101b7303c0ec68bc3fd9113b0d" data-ga-click="Repository, click unstar button, action:blob#show; text:Unstar">        <svg height="16" class="octicon octicon-star-fill" viewBox="0 0 16 16" version="1.1" width="16" aria-hidden="true"><path fill-rule="evenodd" d="M8 .25a.75.75 0 01.673.418l1.882 3.815 4.21.612a.75.75 0 01.416 1.279l-3.046 2.97.719 4.192a.75.75 0 01-1.088.791L8 12.347l-3.766 1.98a.75.75 0 01-1.088-.79l.72-4.194L.818 6.374a.75.75 0 01.416-1.28l4.21-.611L7.327.668A.75.75 0 018 .25z"></path></svg>
        Unstar
</button>        <a class="social-count js-social-count" href="/VolpeUSDOT/FTOT/stargazers"
           aria-label="0 users starred this repository">
           0
        </a>
</form>
    <form class="unstarred js-social-form" action="/VolpeUSDOT/FTOT/star" accept-charset="UTF-8" method="post"><input type="hidden" name="authenticity_token" value="dNGGCARIdWKwHOAuAnu99GXU0esfHmbwKDPEaXBYcEjyYNyXlLuY0FNwAPLAgmc82MRUkGHfLs7m7pPR9c6LQQ==" />
      <input type="hidden" name="context" value="repository"></input>
      <button type="submit" class="btn btn-sm btn-with-count  js-toggler-target" aria-label="Unstar this repository" title="Star VolpeUSDOT/FTOT" data-hydro-click="{&quot;event_type&quot;:&quot;repository.click&quot;,&quot;payload&quot;:{&quot;target&quot;:&quot;STAR_BUTTON&quot;,&quot;repository_id&quot;:162637563,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="39082e15aadeb39cdb461b41d7b7f0df018c8bcbd9b8a89a528cf4d73af5e145" data-ga-click="Repository, click star button, action:blob#show; text:Star">        <svg height="16" class="octicon octicon-star" viewBox="0 0 16 16" version="1.1" width="16" aria-hidden="true"><path fill-rule="evenodd" d="M8 .25a.75.75 0 01.673.418l1.882 3.815 4.21.612a.75.75 0 01.416 1.279l-3.046 2.97.719 4.192a.75.75 0 01-1.088.791L8 12.347l-3.766 1.98a.75.75 0 01-1.088-.79l.72-4.194L.818 6.374a.75.75 0 01.416-1.28l4.21-.611L7.327.668A.75.75 0 018 .25zm0 2.445L6.615 5.5a.75.75 0 01-.564.41l-3.097.45 2.24 2.184a.75.75 0 01.216.664l-.528 3.084 2.769-1.456a.75.75 0 01.698 0l2.77 1.456-.53-3.084a.75.75 0 01.216-.664l2.24-2.183-3.096-.45a.75.75 0 01-.564-.41L8 2.694v.001z"></path></svg>
        Star
</button>        <a class="social-count js-social-count" href="/VolpeUSDOT/FTOT/stargazers"
           aria-label="0 users starred this repository">
          0
        </a>
</form>  </div>

  </li>

  <li>
            <div class="float-left">
              <details class="details-reset details-overlay details-overlay-dark " >
                      <summary
                    class="btn btn-sm btn-with-count"
                    title="Fork your own copy of VolpeUSDOT/FTOT to your account"
                    data-hydro-click="{&quot;event_type&quot;:&quot;repository.click&quot;,&quot;payload&quot;:{&quot;target&quot;:&quot;FORK_BUTTON&quot;,&quot;repository_id&quot;:162637563,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="52c5d40f29166c6c448eed9be03532626802fd4626897a03532d23ee4b1e90ab" data-ga-click="Repository, show fork modal, action:blob#show; text:Fork">
                    <svg class="octicon octicon-repo-forked" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M5 3.25a.75.75 0 11-1.5 0 .75.75 0 011.5 0zm0 2.122a2.25 2.25 0 10-1.5 0v.878A2.25 2.25 0 005.75 8.5h1.5v2.128a2.251 2.251 0 101.5 0V8.5h1.5a2.25 2.25 0 002.25-2.25v-.878a2.25 2.25 0 10-1.5 0v.878a.75.75 0 01-.75.75h-4.5A.75.75 0 015 6.25v-.878zm3.75 7.378a.75.75 0 11-1.5 0 .75.75 0 011.5 0zm3-8.75a.75.75 0 100-1.5.75.75 0 000 1.5z"></path></svg>
                    Fork
                  </summary>

  <details-dialog
    class="Box d-flex flex-column anim-fade-in fast Box--overlay"
      aria-label="Fork FTOT"
      src="/VolpeUSDOT/FTOT/fork?fragment=1"
      preload
    >
    <div class="Box-header">
      <button class="Box-btn-octicon btn-octicon float-right" type="button" aria-label="Close dialog" data-close-dialog>
        <svg height="16" class="octicon octicon-x" viewBox="0 0 16 16" version="1.1" width="16" aria-hidden="true"><path fill-rule="evenodd" d="M3.72 3.72a.75.75 0 011.06 0L8 6.94l3.22-3.22a.75.75 0 111.06 1.06L9.06 8l3.22 3.22a.75.75 0 11-1.06 1.06L8 9.06l-3.22 3.22a.75.75 0 01-1.06-1.06L6.94 8 3.72 4.78a.75.75 0 010-1.06z"></path></svg>
      </button>
      <h1 class="Box-title">Fork FTOT</h1>
    </div>
      
                <div class="text-center overflow-auto">
                  <include-fragment>
                    <div class="octocat-spinner my-5" aria-label="Loading..."></div>
                    <p class="f5 text-gray">If this dialog fails to load, you can visit <a href="/VolpeUSDOT/FTOT/fork">the fork page</a> directly.</p>
                  </include-fragment>
                </div>

  </details-dialog>
</details>
            </div>

      <a href="/VolpeUSDOT/FTOT/network/members" class="social-count"
         aria-label="2 users forked this repository">
        2
      </a>
  </li>
</ul>

      </div>
        
<nav aria-label="Repository" data-pjax="#js-repo-pjax-container" class="js-repo-nav js-sidenav-container-pjax js-responsive-underlinenav overflow-hidden UnderlineNav px-3 px-md-4 px-lg-5 bg-gray-light">
  <ul class="UnderlineNav-body list-style-none ">
          <li class="d-flex">
        <a class="js-selected-navigation-item selected UnderlineNav-item hx_underlinenav-item no-wrap js-responsive-underlinenav-item" data-tab-item="code-tab" data-hotkey="g c" data-ga-click="Repository, Navigation click, Code tab" aria-current="page" data-selected-links="repo_source repo_downloads repo_commits repo_releases repo_tags repo_branches repo_packages repo_deployments /VolpeUSDOT/FTOT" href="/VolpeUSDOT/FTOT">
              <svg classes="UnderlineNav-octicon" display="none inline" height="16" class="octicon octicon-code UnderlineNav-octicon d-none d-sm-inline" viewBox="0 0 16 16" version="1.1" width="16" aria-hidden="true"><path fill-rule="evenodd" d="M4.72 3.22a.75.75 0 011.06 1.06L2.06 8l3.72 3.72a.75.75 0 11-1.06 1.06L.47 8.53a.75.75 0 010-1.06l4.25-4.25zm6.56 0a.75.75 0 10-1.06 1.06L13.94 8l-3.72 3.72a.75.75 0 101.06 1.06l4.25-4.25a.75.75 0 000-1.06l-4.25-4.25z"></path></svg>
            <span data-content="Code">Code</span>
              <span title="Not available" class="Counter "></span>
</a>      </li>
      <li class="d-flex">
        <a class="js-selected-navigation-item UnderlineNav-item hx_underlinenav-item no-wrap js-responsive-underlinenav-item" data-tab-item="issues-tab" data-hotkey="g i" data-ga-click="Repository, Navigation click, Issues tab" data-selected-links="repo_issues repo_labels repo_milestones /VolpeUSDOT/FTOT/issues" href="/VolpeUSDOT/FTOT/issues">
              <svg classes="UnderlineNav-octicon" display="none inline" height="16" class="octicon octicon-issue-opened UnderlineNav-octicon d-none d-sm-inline" viewBox="0 0 16 16" version="1.1" width="16" aria-hidden="true"><path fill-rule="evenodd" d="M8 1.5a6.5 6.5 0 100 13 6.5 6.5 0 000-13zM0 8a8 8 0 1116 0A8 8 0 010 8zm9 3a1 1 0 11-2 0 1 1 0 012 0zm-.25-6.25a.75.75 0 00-1.5 0v3.5a.75.75 0 001.5 0v-3.5z"></path></svg>
            <span data-content="Issues">Issues</span>
              <span title="87" class="Counter ">87</span>
</a>      </li>
      <li class="d-flex">
        <a class="js-selected-navigation-item UnderlineNav-item hx_underlinenav-item no-wrap js-responsive-underlinenav-item" data-tab-item="pull-requests-tab" data-hotkey="g p" data-ga-click="Repository, Navigation click, Pull requests tab" data-selected-links="repo_pulls checks /VolpeUSDOT/FTOT/pulls" href="/VolpeUSDOT/FTOT/pulls">
              <svg classes="UnderlineNav-octicon" display="none inline" height="16" class="octicon octicon-git-pull-request UnderlineNav-octicon d-none d-sm-inline" viewBox="0 0 16 16" version="1.1" width="16" aria-hidden="true"><path fill-rule="evenodd" d="M7.177 3.073L9.573.677A.25.25 0 0110 .854v4.792a.25.25 0 01-.427.177L7.177 3.427a.25.25 0 010-.354zM3.75 2.5a.75.75 0 100 1.5.75.75 0 000-1.5zm-2.25.75a2.25 2.25 0 113 2.122v5.256a2.251 2.251 0 11-1.5 0V5.372A2.25 2.25 0 011.5 3.25zM11 2.5h-1V4h1a1 1 0 011 1v5.628a2.251 2.251 0 101.5 0V5A2.5 2.5 0 0011 2.5zm1 10.25a.75.75 0 111.5 0 .75.75 0 01-1.5 0zM3.75 12a.75.75 0 100 1.5.75.75 0 000-1.5z"></path></svg>
            <span data-content="Pull requests">Pull requests</span>
              <span title="0" hidden="hidden" class="Counter ">0</span>
</a>      </li>
      <li class="d-flex">
        <a class="js-selected-navigation-item UnderlineNav-item hx_underlinenav-item no-wrap js-responsive-underlinenav-item" data-tab-item="actions-tab" data-hotkey="g a" data-ga-click="Repository, Navigation click, Actions tab" data-selected-links="repo_actions /VolpeUSDOT/FTOT/actions" href="/VolpeUSDOT/FTOT/actions">
              <svg classes="UnderlineNav-octicon" display="none inline" height="16" class="octicon octicon-play UnderlineNav-octicon d-none d-sm-inline" viewBox="0 0 16 16" version="1.1" width="16" aria-hidden="true"><path fill-rule="evenodd" d="M1.5 8a6.5 6.5 0 1113 0 6.5 6.5 0 01-13 0zM8 0a8 8 0 100 16A8 8 0 008 0zM6.379 5.227A.25.25 0 006 5.442v5.117a.25.25 0 00.379.214l4.264-2.559a.25.25 0 000-.428L6.379 5.227z"></path></svg>
            <span data-content="Actions">Actions</span>
              <span title="Not available" class="Counter "></span>
</a>      </li>
      <li class="d-flex">
        <a class="js-selected-navigation-item UnderlineNav-item hx_underlinenav-item no-wrap js-responsive-underlinenav-item" data-tab-item="projects-tab" data-hotkey="g b" data-ga-click="Repository, Navigation click, Projects tab" data-selected-links="repo_projects new_repo_project repo_project /VolpeUSDOT/FTOT/projects" href="/VolpeUSDOT/FTOT/projects">
              <svg classes="UnderlineNav-octicon" display="none inline" height="16" class="octicon octicon-project UnderlineNav-octicon d-none d-sm-inline" viewBox="0 0 16 16" version="1.1" width="16" aria-hidden="true"><path fill-rule="evenodd" d="M1.75 0A1.75 1.75 0 000 1.75v12.5C0 15.216.784 16 1.75 16h12.5A1.75 1.75 0 0016 14.25V1.75A1.75 1.75 0 0014.25 0H1.75zM1.5 1.75a.25.25 0 01.25-.25h12.5a.25.25 0 01.25.25v12.5a.25.25 0 01-.25.25H1.75a.25.25 0 01-.25-.25V1.75zM11.75 3a.75.75 0 00-.75.75v7.5a.75.75 0 001.5 0v-7.5a.75.75 0 00-.75-.75zm-8.25.75a.75.75 0 011.5 0v5.5a.75.75 0 01-1.5 0v-5.5zM8 3a.75.75 0 00-.75.75v3.5a.75.75 0 001.5 0v-3.5A.75.75 0 008 3z"></path></svg>
            <span data-content="Projects">Projects</span>
              <span title="2" class="Counter ">2</span>
</a>      </li>
      <li class="d-flex">
        <a class="js-selected-navigation-item UnderlineNav-item hx_underlinenav-item no-wrap js-responsive-underlinenav-item" data-tab-item="wiki-tab" data-hotkey="g w" data-ga-click="Repository, Navigation click, Wikis tab" data-selected-links="repo_wiki /VolpeUSDOT/FTOT/wiki" href="/VolpeUSDOT/FTOT/wiki">
              <svg classes="UnderlineNav-octicon" display="none inline" height="16" class="octicon octicon-book UnderlineNav-octicon d-none d-sm-inline" viewBox="0 0 16 16" version="1.1" width="16" aria-hidden="true"><path fill-rule="evenodd" d="M0 1.75A.75.75 0 01.75 1h4.253c1.227 0 2.317.59 3 1.501A3.744 3.744 0 0111.006 1h4.245a.75.75 0 01.75.75v10.5a.75.75 0 01-.75.75h-4.507a2.25 2.25 0 00-1.591.659l-.622.621a.75.75 0 01-1.06 0l-.622-.621A2.25 2.25 0 005.258 13H.75a.75.75 0 01-.75-.75V1.75zm8.755 3a2.25 2.25 0 012.25-2.25H14.5v9h-3.757c-.71 0-1.4.201-1.992.572l.004-7.322zm-1.504 7.324l.004-5.073-.002-2.253A2.25 2.25 0 005.003 2.5H1.5v9h3.757a3.75 3.75 0 011.994.574z"></path></svg>
            <span data-content="Wiki">Wiki</span>
              <span title="Not available" class="Counter "></span>
</a>      </li>
      <li class="d-flex">
        <a class="js-selected-navigation-item UnderlineNav-item hx_underlinenav-item no-wrap js-responsive-underlinenav-item" data-tab-item="security-tab" data-hotkey="g s" data-ga-click="Repository, Navigation click, Security tab" data-selected-links="security overview alerts policy token_scanning code_scanning /VolpeUSDOT/FTOT/security" href="/VolpeUSDOT/FTOT/security">
              <svg classes="UnderlineNav-octicon" display="none inline" height="16" class="octicon octicon-shield UnderlineNav-octicon d-none d-sm-inline" viewBox="0 0 16 16" version="1.1" width="16" aria-hidden="true"><path fill-rule="evenodd" d="M7.467.133a1.75 1.75 0 011.066 0l5.25 1.68A1.75 1.75 0 0115 3.48V7c0 1.566-.32 3.182-1.303 4.682-.983 1.498-2.585 2.813-5.032 3.855a1.7 1.7 0 01-1.33 0c-2.447-1.042-4.049-2.357-5.032-3.855C1.32 10.182 1 8.566 1 7V3.48a1.75 1.75 0 011.217-1.667l5.25-1.68zm.61 1.429a.25.25 0 00-.153 0l-5.25 1.68a.25.25 0 00-.174.238V7c0 1.358.275 2.666 1.057 3.86.784 1.194 2.121 2.34 4.366 3.297a.2.2 0 00.154 0c2.245-.956 3.582-2.104 4.366-3.298C13.225 9.666 13.5 8.36 13.5 7V3.48a.25.25 0 00-.174-.237l-5.25-1.68zM9 10.5a1 1 0 11-2 0 1 1 0 012 0zm-.25-5.75a.75.75 0 10-1.5 0v3a.75.75 0 001.5 0v-3z"></path></svg>
            <span data-content="Security">Security</span>
              <include-fragment src="/VolpeUSDOT/FTOT/security/overall-count" accept="text/fragment+html"></include-fragment>
</a>      </li>
      <li class="d-flex">
        <a class="js-selected-navigation-item UnderlineNav-item hx_underlinenav-item no-wrap js-responsive-underlinenav-item" data-tab-item="insights-tab" data-ga-click="Repository, Navigation click, Insights tab" data-selected-links="repo_graphs repo_contributors dependency_graph dependabot_updates pulse people /VolpeUSDOT/FTOT/pulse" href="/VolpeUSDOT/FTOT/pulse">
              <svg classes="UnderlineNav-octicon" display="none inline" height="16" class="octicon octicon-graph UnderlineNav-octicon d-none d-sm-inline" viewBox="0 0 16 16" version="1.1" width="16" aria-hidden="true"><path fill-rule="evenodd" d="M1.5 1.75a.75.75 0 00-1.5 0v12.5c0 .414.336.75.75.75h14.5a.75.75 0 000-1.5H1.5V1.75zm14.28 2.53a.75.75 0 00-1.06-1.06L10 7.94 7.53 5.47a.75.75 0 00-1.06 0L3.22 8.72a.75.75 0 001.06 1.06L7 7.06l2.47 2.47a.75.75 0 001.06 0l5.25-5.25z"></path></svg>
            <span data-content="Insights">Insights</span>
              <span title="Not available" class="Counter "></span>
</a>      </li>
      <li class="d-flex">
        <a class="js-selected-navigation-item UnderlineNav-item hx_underlinenav-item no-wrap js-responsive-underlinenav-item" data-tab-item="settings-tab" data-ga-click="Repository, Navigation click, Settings tab" data-selected-links="repo_settings repo_branch_settings hooks integration_installations repo_keys_settings issue_template_editor secrets_settings key_links_settings repo_actions_settings notifications /VolpeUSDOT/FTOT/settings" href="/VolpeUSDOT/FTOT/settings">
              <svg classes="UnderlineNav-octicon" display="none inline" height="16" class="octicon octicon-gear UnderlineNav-octicon d-none d-sm-inline" viewBox="0 0 16 16" version="1.1" width="16" aria-hidden="true"><path fill-rule="evenodd" d="M7.429 1.525a6.593 6.593 0 011.142 0c.036.003.108.036.137.146l.289 1.105c.147.56.55.967.997 1.189.174.086.341.183.501.29.417.278.97.423 1.53.27l1.102-.303c.11-.03.175.016.195.046.219.31.41.641.573.989.014.031.022.11-.059.19l-.815.806c-.411.406-.562.957-.53 1.456a4.588 4.588 0 010 .582c-.032.499.119 1.05.53 1.456l.815.806c.08.08.073.159.059.19a6.494 6.494 0 01-.573.99c-.02.029-.086.074-.195.045l-1.103-.303c-.559-.153-1.112-.008-1.529.27-.16.107-.327.204-.5.29-.449.222-.851.628-.998 1.189l-.289 1.105c-.029.11-.101.143-.137.146a6.613 6.613 0 01-1.142 0c-.036-.003-.108-.037-.137-.146l-.289-1.105c-.147-.56-.55-.967-.997-1.189a4.502 4.502 0 01-.501-.29c-.417-.278-.97-.423-1.53-.27l-1.102.303c-.11.03-.175-.016-.195-.046a6.492 6.492 0 01-.573-.989c-.014-.031-.022-.11.059-.19l.815-.806c.411-.406.562-.957.53-1.456a4.587 4.587 0 010-.582c.032-.499-.119-1.05-.53-1.456l-.815-.806c-.08-.08-.073-.159-.059-.19a6.44 6.44 0 01.573-.99c.02-.029.086-.075.195-.045l1.103.303c.559.153 1.112.008 1.529-.27.16-.107.327-.204.5-.29.449-.222.851-.628.998-1.189l.289-1.105c.029-.11.101-.143.137-.146zM8 0c-.236 0-.47.01-.701.03-.743.065-1.29.615-1.458 1.261l-.29 1.106c-.017.066-.078.158-.211.224a5.994 5.994 0 00-.668.386c-.123.082-.233.09-.3.071L3.27 2.776c-.644-.177-1.392.02-1.82.63a7.977 7.977 0 00-.704 1.217c-.315.675-.111 1.422.363 1.891l.815.806c.05.048.098.147.088.294a6.084 6.084 0 000 .772c.01.147-.038.246-.088.294l-.815.806c-.474.469-.678 1.216-.363 1.891.2.428.436.835.704 1.218.428.609 1.176.806 1.82.63l1.103-.303c.066-.019.176-.011.299.071.213.143.436.272.668.386.133.066.194.158.212.224l.289 1.106c.169.646.715 1.196 1.458 1.26a8.094 8.094 0 001.402 0c.743-.064 1.29-.614 1.458-1.26l.29-1.106c.017-.066.078-.158.211-.224a5.98 5.98 0 00.668-.386c.123-.082.233-.09.3-.071l1.102.302c.644.177 1.392-.02 1.82-.63.268-.382.505-.789.704-1.217.315-.675.111-1.422-.364-1.891l-.814-.806c-.05-.048-.098-.147-.088-.294a6.1 6.1 0 000-.772c-.01-.147.039-.246.088-.294l.814-.806c.475-.469.679-1.216.364-1.891a7.992 7.992 0 00-.704-1.218c-.428-.609-1.176-.806-1.82-.63l-1.103.303c-.066.019-.176.011-.299-.071a5.991 5.991 0 00-.668-.386c-.133-.066-.194-.158-.212-.224L10.16 1.29C9.99.645 9.444.095 8.701.031A8.094 8.094 0 008 0zm1.5 8a1.5 1.5 0 11-3 0 1.5 1.5 0 013 0zM11 8a3 3 0 11-6 0 3 3 0 016 0z"></path></svg>
            <span data-content="Settings">Settings</span>
              <span title="Not available" class="Counter "></span>
</a>      </li>

</ul>        <div class="position-absolute right-0 pr-3 pr-md-4 pr-lg-5 js-responsive-underlinenav-overflow" style="visibility:hidden;">
      <details class="details-overlay details-reset position-relative">
  <summary role="button">
    <div class="UnderlineNav-item mr-0 border-0">
            <svg class="octicon octicon-kebab-horizontal" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path d="M8 9a1.5 1.5 0 100-3 1.5 1.5 0 000 3zM1.5 9a1.5 1.5 0 100-3 1.5 1.5 0 000 3zm13 0a1.5 1.5 0 100-3 1.5 1.5 0 000 3z"></path></svg>
            <span class="sr-only">More</span>
          </div>
</summary>  <div>
    <details-menu role="menu" class="dropdown-menu dropdown-menu-sw ">
  
            <ul>
                <li data-menu-item="code-tab" hidden>
                  <a role="menuitem" class="js-selected-navigation-item dropdown-item" data-selected-links=" /VolpeUSDOT/FTOT" href="/VolpeUSDOT/FTOT">
                    Code
</a>                </li>
                <li data-menu-item="issues-tab" hidden>
                  <a role="menuitem" class="js-selected-navigation-item dropdown-item" data-selected-links=" /VolpeUSDOT/FTOT/issues" href="/VolpeUSDOT/FTOT/issues">
                    Issues
</a>                </li>
                <li data-menu-item="pull-requests-tab" hidden>
                  <a role="menuitem" class="js-selected-navigation-item dropdown-item" data-selected-links=" /VolpeUSDOT/FTOT/pulls" href="/VolpeUSDOT/FTOT/pulls">
                    Pull requests
</a>                </li>
                <li data-menu-item="actions-tab" hidden>
                  <a role="menuitem" class="js-selected-navigation-item dropdown-item" data-selected-links=" /VolpeUSDOT/FTOT/actions" href="/VolpeUSDOT/FTOT/actions">
                    Actions
</a>                </li>
                <li data-menu-item="projects-tab" hidden>
                  <a role="menuitem" class="js-selected-navigation-item dropdown-item" data-selected-links=" /VolpeUSDOT/FTOT/projects" href="/VolpeUSDOT/FTOT/projects">
                    Projects
</a>                </li>
                <li data-menu-item="wiki-tab" hidden>
                  <a role="menuitem" class="js-selected-navigation-item dropdown-item" data-selected-links=" /VolpeUSDOT/FTOT/wiki" href="/VolpeUSDOT/FTOT/wiki">
                    Wiki
</a>                </li>
                <li data-menu-item="security-tab" hidden>
                  <a role="menuitem" class="js-selected-navigation-item dropdown-item" data-selected-links=" /VolpeUSDOT/FTOT/security" href="/VolpeUSDOT/FTOT/security">
                    Security
</a>                </li>
                <li data-menu-item="insights-tab" hidden>
                  <a role="menuitem" class="js-selected-navigation-item dropdown-item" data-selected-links=" /VolpeUSDOT/FTOT/pulse" href="/VolpeUSDOT/FTOT/pulse">
                    Insights
</a>                </li>
                <li data-menu-item="settings-tab" hidden>
                  <a role="menuitem" class="js-selected-navigation-item dropdown-item" data-selected-links=" /VolpeUSDOT/FTOT/settings" href="/VolpeUSDOT/FTOT/settings">
                    Settings
</a>                </li>
            </ul>

</details-menu>
</div></details>    </div>

</nav>
  </div>


<div class="container-xl clearfix new-discussion-timeline px-3 px-md-4 px-lg-5">
  <div class="repository-content " >

    
      
  


    <a class="d-none js-permalink-shortcut" data-hotkey="y" href="/VolpeUSDOT/FTOT/blob/d9a19dc4879590346cf9ff899019f39a2f6dd14e/program/ftot_routing.py">Permalink</a>

    <!-- blob contrib key: blob_contributors:v22:86d072b85324ea5c794bc1bcc70221ad275c89a3f06a295df844cff0287c798a -->
    

    <div class="d-flex flex-items-start flex-shrink-0 pb-3 flex-wrap flex-md-nowrap flex-justify-between flex-md-justify-start">
      
<div class="position-relative">
  <details class="details-reset details-overlay mr-0 mb-0 " id="branch-select-menu">
    <summary class="btn css-truncate"
            data-hotkey="w"
            title="Switch branches or tags">
      <svg text="gray" height="16" class="octicon octicon-git-branch text-gray" viewBox="0 0 16 16" version="1.1" width="16" aria-hidden="true"><path fill-rule="evenodd" d="M11.75 2.5a.75.75 0 100 1.5.75.75 0 000-1.5zm-2.25.75a2.25 2.25 0 113 2.122V6A2.5 2.5 0 0110 8.5H6a1 1 0 00-1 1v1.128a2.251 2.251 0 11-1.5 0V5.372a2.25 2.25 0 111.5 0v1.836A2.492 2.492 0 016 7h4a1 1 0 001-1v-.628A2.25 2.25 0 019.5 3.25zM4.25 12a.75.75 0 100 1.5.75.75 0 000-1.5zM3.5 3.25a.75.75 0 111.5 0 .75.75 0 01-1.5 0z"></path></svg>
      <span class="css-truncate-target" data-menu-button>master</span>
      <span class="dropdown-caret"></span>
    </summary>

    <details-menu class="SelectMenu SelectMenu--hasFilter" src="/VolpeUSDOT/FTOT/refs/master/program/ftot_routing.py?source_action=show&amp;source_controller=blob" preload>
      <div class="SelectMenu-modal">
        <include-fragment class="SelectMenu-loading" aria-label="Menu is loading">
          <svg class="octicon octicon-octoface anim-pulse" height="32" viewBox="0 0 24 24" version="1.1" width="32" aria-hidden="true"><path d="M7.75 11c-.69 0-1.25.56-1.25 1.25v1.5a1.25 1.25 0 102.5 0v-1.5C9 11.56 8.44 11 7.75 11zm1.27 4.5a.469.469 0 01.48-.5h5a.47.47 0 01.48.5c-.116 1.316-.759 2.5-2.98 2.5s-2.864-1.184-2.98-2.5zm7.23-4.5c-.69 0-1.25.56-1.25 1.25v1.5a1.25 1.25 0 102.5 0v-1.5c0-.69-.56-1.25-1.25-1.25z"></path><path fill-rule="evenodd" d="M21.255 3.82a1.725 1.725 0 00-2.141-1.195c-.557.16-1.406.44-2.264.866-.78.386-1.647.93-2.293 1.677A18.442 18.442 0 0012 5c-.93 0-1.784.059-2.569.17-.645-.74-1.505-1.28-2.28-1.664a13.876 13.876 0 00-2.265-.866 1.725 1.725 0 00-2.141 1.196 23.645 23.645 0 00-.69 3.292c-.125.97-.191 2.07-.066 3.112C1.254 11.882 1 13.734 1 15.527 1 19.915 3.13 23 12 23c8.87 0 11-3.053 11-7.473 0-1.794-.255-3.647-.99-5.29.127-1.046.06-2.15-.066-3.125a23.652 23.652 0 00-.689-3.292zM20.5 14c.5 3.5-1.5 6.5-8.5 6.5s-9-3-8.5-6.5c.583-4 3-6 8.5-6s7.928 2 8.5 6z"></path></svg>
        </include-fragment>
      </div>
    </details-menu>
  </details>

</div>

      <h2 id="blob-path" class="breadcrumb flex-auto min-width-0 text-normal mx-0 mx-md-3 width-full width-md-auto flex-order-1 flex-md-order-none mt-3 mt-md-0">
        <span class="js-repo-root text-bold"><span class="js-path-segment d-inline-block wb-break-all"><a data-pjax="true" href="/VolpeUSDOT/FTOT"><span>FTOT</span></a></span></span><span class="separator">/</span><span class="js-path-segment d-inline-block wb-break-all"><a data-pjax="true" href="/VolpeUSDOT/FTOT/tree/master/program"><span>program</span></a></span><span class="separator">/</span><strong class="final-path">ftot_routing.py</strong>
          <span class="separator">/</span><details class="details-reset details-overlay d-inline" id="jumpto-symbol-select-menu">
  <summary class="btn-link link-gray css-truncate" aria-haspopup="true" data-hotkey="r" data-hydro-click="{&quot;event_type&quot;:&quot;code_navigation.click_on_blob_definitions&quot;,&quot;payload&quot;:{&quot;action&quot;:&quot;click_on_blob_definitions&quot;,&quot;repository_id&quot;:162637563,&quot;ref&quot;:&quot;master&quot;,&quot;language&quot;:&quot;Python&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="10cdf8b9b5a05d5df1c34103c195b6a18c87bed39d442e16ba44703c4a5bbe61">
      <svg class="octicon octicon-code" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M4.72 3.22a.75.75 0 011.06 1.06L2.06 8l3.72 3.72a.75.75 0 11-1.06 1.06L.47 8.53a.75.75 0 010-1.06l4.25-4.25zm6.56 0a.75.75 0 10-1.06 1.06L13.94 8l-3.72 3.72a.75.75 0 101.06 1.06l4.25-4.25a.75.75 0 000-1.06l-4.25-4.25z"></path></svg>
    <span data-menu-button>Jump to</span>
    <span class="dropdown-caret"></span>
  </summary>
  <details-menu class="SelectMenu SelectMenu--hasFilter" role="menu">
    <div class="SelectMenu-modal">
      <header class="SelectMenu-header">
        <span class="SelectMenu-title">Code definitions</span>
        <button class="SelectMenu-closeButton" type="button" data-toggle-for="jumpto-symbol-select-menu">
          <svg aria-label="Close menu" class="octicon octicon-x" viewBox="0 0 16 16" version="1.1" width="16" height="16" role="img"><path fill-rule="evenodd" d="M3.72 3.72a.75.75 0 011.06 0L8 6.94l3.22-3.22a.75.75 0 111.06 1.06L9.06 8l3.22 3.22a.75.75 0 11-1.06 1.06L8 9.06l-3.22 3.22a.75.75 0 01-1.06-1.06L6.94 8 3.72 4.78a.75.75 0 010-1.06z"></path></svg>
        </button>
      </header>
        <div class="SelectMenu-filter">
          <input
            class="SelectMenu-input form-control js-filterable-field"
            id="jumpto-symbols-filter-field"
            type="text"
            autocomplete="off"
            spellcheck="false"
            autofocus
            placeholder="Filter definitions"
            aria-label="Filter definitions">
        </div>
      <div class="SelectMenu-list">
        <div data-filterable-for="jumpto-symbols-filter-field" data-filterable-type="substring">
            <a class="SelectMenu-item d-flex flex-justify-between css-truncate" role="menuitemradio" aria-checked="false" rel="nofollow" data-hydro-click="{&quot;event_type&quot;:&quot;code_navigation.navigate_to_blob_definition&quot;,&quot;payload&quot;:{&quot;action&quot;:&quot;navigate_to_blob_definition&quot;,&quot;repository_id&quot;:162637563,&quot;ref&quot;:&quot;master&quot;,&quot;language&quot;:&quot;Python&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="049b342901063a8d9121d21929bbae1c17af599418f1160f0653508181e24dd0" href="/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py#L25">
              <svg class="octicon octicon-check SelectMenu-icon SelectMenu-icon--check" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M13.78 4.22a.75.75 0 010 1.06l-7.25 7.25a.75.75 0 01-1.06 0L2.22 9.28a.75.75 0 011.06-1.06L6 10.94l6.72-6.72a.75.75 0 011.06 0z"></path></svg>
              <span class="flex-auto css-truncate-target" data-menu-button-text>connectivity</span>
              <span class="flex-auto d-flex flex-justify-end">Function</span>
</a>            <a class="SelectMenu-item d-flex flex-justify-between css-truncate" role="menuitemradio" aria-checked="false" rel="nofollow" data-hydro-click="{&quot;event_type&quot;:&quot;code_navigation.navigate_to_blob_definition&quot;,&quot;payload&quot;:{&quot;action&quot;:&quot;navigate_to_blob_definition&quot;,&quot;repository_id&quot;:162637563,&quot;ref&quot;:&quot;master&quot;,&quot;language&quot;:&quot;Python&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="049b342901063a8d9121d21929bbae1c17af599418f1160f0653508181e24dd0" href="/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py#L51">
              <svg class="octicon octicon-check SelectMenu-icon SelectMenu-icon--check" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M13.78 4.22a.75.75 0 010 1.06l-7.25 7.25a.75.75 0 01-1.06 0L2.22 9.28a.75.75 0 011.06-1.06L6 10.94l6.72-6.72a.75.75 0 011.06 0z"></path></svg>
              <span class="flex-auto css-truncate-target" data-menu-button-text>checks_and_cleanup</span>
              <span class="flex-auto d-flex flex-justify-end">Function</span>
</a>            <a class="SelectMenu-item d-flex flex-justify-between css-truncate" role="menuitemradio" aria-checked="false" rel="nofollow" data-hydro-click="{&quot;event_type&quot;:&quot;code_navigation.navigate_to_blob_definition&quot;,&quot;payload&quot;:{&quot;action&quot;:&quot;navigate_to_blob_definition&quot;,&quot;repository_id&quot;:162637563,&quot;ref&quot;:&quot;master&quot;,&quot;language&quot;:&quot;Python&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="049b342901063a8d9121d21929bbae1c17af599418f1160f0653508181e24dd0" href="/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py#L77">
              <svg class="octicon octicon-check SelectMenu-icon SelectMenu-icon--check" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M13.78 4.22a.75.75 0 010 1.06l-7.25 7.25a.75.75 0 01-1.06 0L2.22 9.28a.75.75 0 011.06-1.06L6 10.94l6.72-6.72a.75.75 0 011.06 0z"></path></svg>
              <span class="flex-auto css-truncate-target" data-menu-button-text>create_locations_fc</span>
              <span class="flex-auto d-flex flex-justify-end">Function</span>
</a>            <a class="SelectMenu-item d-flex flex-justify-between css-truncate" role="menuitemradio" aria-checked="false" rel="nofollow" data-hydro-click="{&quot;event_type&quot;:&quot;code_navigation.navigate_to_blob_definition&quot;,&quot;payload&quot;:{&quot;action&quot;:&quot;navigate_to_blob_definition&quot;,&quot;repository_id&quot;:162637563,&quot;ref&quot;:&quot;master&quot;,&quot;language&quot;:&quot;Python&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="049b342901063a8d9121d21929bbae1c17af599418f1160f0653508181e24dd0" href="/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py#L159">
              <svg class="octicon octicon-check SelectMenu-icon SelectMenu-icon--check" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M13.78 4.22a.75.75 0 010 1.06l-7.25 7.25a.75.75 0 01-1.06 0L2.22 9.28a.75.75 0 011.06-1.06L6 10.94l6.72-6.72a.75.75 0 011.06 0z"></path></svg>
              <span class="flex-auto css-truncate-target" data-menu-button-text>get_xy_location_id_dict</span>
              <span class="flex-auto d-flex flex-justify-end">Function</span>
</a>            <a class="SelectMenu-item d-flex flex-justify-between css-truncate" role="menuitemradio" aria-checked="false" rel="nofollow" data-hydro-click="{&quot;event_type&quot;:&quot;code_navigation.navigate_to_blob_definition&quot;,&quot;payload&quot;:{&quot;action&quot;:&quot;navigate_to_blob_definition&quot;,&quot;repository_id&quot;:162637563,&quot;ref&quot;:&quot;master&quot;,&quot;language&quot;:&quot;Python&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="049b342901063a8d9121d21929bbae1c17af599418f1160f0653508181e24dd0" href="/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py#L182">
              <svg class="octicon octicon-check SelectMenu-icon SelectMenu-icon--check" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M13.78 4.22a.75.75 0 010 1.06l-7.25 7.25a.75.75 0 01-1.06 0L2.22 9.28a.75.75 0 011.06-1.06L6 10.94l6.72-6.72a.75.75 0 011.06 0z"></path></svg>
              <span class="flex-auto css-truncate-target" data-menu-button-text>get_location_id_name_dict</span>
              <span class="flex-auto d-flex flex-justify-end">Function</span>
</a>            <a class="SelectMenu-item d-flex flex-justify-between css-truncate" role="menuitemradio" aria-checked="false" rel="nofollow" data-hydro-click="{&quot;event_type&quot;:&quot;code_navigation.navigate_to_blob_definition&quot;,&quot;payload&quot;:{&quot;action&quot;:&quot;navigate_to_blob_definition&quot;,&quot;repository_id&quot;:162637563,&quot;ref&quot;:&quot;master&quot;,&quot;language&quot;:&quot;Python&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="049b342901063a8d9121d21929bbae1c17af599418f1160f0653508181e24dd0" href="/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py#L203">
              <svg class="octicon octicon-check SelectMenu-icon SelectMenu-icon--check" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M13.78 4.22a.75.75 0 010 1.06l-7.25 7.25a.75.75 0 01-1.06 0L2.22 9.28a.75.75 0 011.06-1.06L6 10.94l6.72-6.72a.75.75 0 011.06 0z"></path></svg>
              <span class="flex-auto css-truncate-target" data-menu-button-text>delete_old_artificial_link</span>
              <span class="flex-auto d-flex flex-justify-end">Function</span>
</a>            <a class="SelectMenu-item d-flex flex-justify-between css-truncate" role="menuitemradio" aria-checked="false" rel="nofollow" data-hydro-click="{&quot;event_type&quot;:&quot;code_navigation.navigate_to_blob_definition&quot;,&quot;payload&quot;:{&quot;action&quot;:&quot;navigate_to_blob_definition&quot;,&quot;repository_id&quot;:162637563,&quot;ref&quot;:&quot;master&quot;,&quot;language&quot;:&quot;Python&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="049b342901063a8d9121d21929bbae1c17af599418f1160f0653508181e24dd0" href="/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py#L223">
              <svg class="octicon octicon-check SelectMenu-icon SelectMenu-icon--check" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M13.78 4.22a.75.75 0 010 1.06l-7.25 7.25a.75.75 0 01-1.06 0L2.22 9.28a.75.75 0 011.06-1.06L6 10.94l6.72-6.72a.75.75 0 011.06 0z"></path></svg>
              <span class="flex-auto css-truncate-target" data-menu-button-text>cut_lines</span>
              <span class="flex-auto d-flex flex-justify-end">Function</span>
</a>            <a class="SelectMenu-item d-flex flex-justify-between css-truncate" role="menuitemradio" aria-checked="false" rel="nofollow" data-hydro-click="{&quot;event_type&quot;:&quot;code_navigation.navigate_to_blob_definition&quot;,&quot;payload&quot;:{&quot;action&quot;:&quot;navigate_to_blob_definition&quot;,&quot;repository_id&quot;:162637563,&quot;ref&quot;:&quot;master&quot;,&quot;language&quot;:&quot;Python&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="049b342901063a8d9121d21929bbae1c17af599418f1160f0653508181e24dd0" href="/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py#L278">
              <svg class="octicon octicon-check SelectMenu-icon SelectMenu-icon--check" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M13.78 4.22a.75.75 0 010 1.06l-7.25 7.25a.75.75 0 01-1.06 0L2.22 9.28a.75.75 0 011.06-1.06L6 10.94l6.72-6.72a.75.75 0 011.06 0z"></path></svg>
              <span class="flex-auto css-truncate-target" data-menu-button-text>hook_locations_into_network</span>
              <span class="flex-auto d-flex flex-justify-end">Function</span>
</a>            <a class="SelectMenu-item d-flex flex-justify-between css-truncate" role="menuitemradio" aria-checked="false" rel="nofollow" data-hydro-click="{&quot;event_type&quot;:&quot;code_navigation.navigate_to_blob_definition&quot;,&quot;payload&quot;:{&quot;action&quot;:&quot;navigate_to_blob_definition&quot;,&quot;repository_id&quot;:162637563,&quot;ref&quot;:&quot;master&quot;,&quot;language&quot;:&quot;Python&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="049b342901063a8d9121d21929bbae1c17af599418f1160f0653508181e24dd0" href="/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py#L330">
              <svg class="octicon octicon-check SelectMenu-icon SelectMenu-icon--check" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M13.78 4.22a.75.75 0 010 1.06l-7.25 7.25a.75.75 0 01-1.06 0L2.22 9.28a.75.75 0 011.06-1.06L6 10.94l6.72-6.72a.75.75 0 011.06 0z"></path></svg>
              <span class="flex-auto css-truncate-target" data-menu-button-text>cache_capacity_information</span>
              <span class="flex-auto d-flex flex-justify-end">Function</span>
</a>            <a class="SelectMenu-item d-flex flex-justify-between css-truncate" role="menuitemradio" aria-checked="false" rel="nofollow" data-hydro-click="{&quot;event_type&quot;:&quot;code_navigation.navigate_to_blob_definition&quot;,&quot;payload&quot;:{&quot;action&quot;:&quot;navigate_to_blob_definition&quot;,&quot;repository_id&quot;:162637563,&quot;ref&quot;:&quot;master&quot;,&quot;language&quot;:&quot;Python&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="049b342901063a8d9121d21929bbae1c17af599418f1160f0653508181e24dd0" href="/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py#L440">
              <svg class="octicon octicon-check SelectMenu-icon SelectMenu-icon--check" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M13.78 4.22a.75.75 0 010 1.06l-7.25 7.25a.75.75 0 01-1.06 0L2.22 9.28a.75.75 0 011.06-1.06L6 10.94l6.72-6.72a.75.75 0 011.06 0z"></path></svg>
              <span class="flex-auto css-truncate-target" data-menu-button-text>locations_add_links</span>
              <span class="flex-auto d-flex flex-justify-end">Function</span>
</a>            <a class="SelectMenu-item d-flex flex-justify-between css-truncate" role="menuitemradio" aria-checked="false" rel="nofollow" data-hydro-click="{&quot;event_type&quot;:&quot;code_navigation.navigate_to_blob_definition&quot;,&quot;payload&quot;:{&quot;action&quot;:&quot;navigate_to_blob_definition&quot;,&quot;repository_id&quot;:162637563,&quot;ref&quot;:&quot;master&quot;,&quot;language&quot;:&quot;Python&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="049b342901063a8d9121d21929bbae1c17af599418f1160f0653508181e24dd0" href="/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py#L987">
              <svg class="octicon octicon-check SelectMenu-icon SelectMenu-icon--check" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M13.78 4.22a.75.75 0 010 1.06l-7.25 7.25a.75.75 0 01-1.06 0L2.22 9.28a.75.75 0 011.06-1.06L6 10.94l6.72-6.72a.75.75 0 011.06 0z"></path></svg>
              <span class="flex-auto css-truncate-target" data-menu-button-text>ignore_locations_not_connected_to_network</span>
              <span class="flex-auto d-flex flex-justify-end">Function</span>
</a>            <a class="SelectMenu-item d-flex flex-justify-between css-truncate" role="menuitemradio" aria-checked="false" rel="nofollow" data-hydro-click="{&quot;event_type&quot;:&quot;code_navigation.navigate_to_blob_definition&quot;,&quot;payload&quot;:{&quot;action&quot;:&quot;navigate_to_blob_definition&quot;,&quot;repository_id&quot;:162637563,&quot;ref&quot;:&quot;master&quot;,&quot;language&quot;:&quot;Python&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="049b342901063a8d9121d21929bbae1c17af599418f1160f0653508181e24dd0" href="/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py#L1075">
              <svg class="octicon octicon-check SelectMenu-icon SelectMenu-icon--check" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M13.78 4.22a.75.75 0 010 1.06l-7.25 7.25a.75.75 0 01-1.06 0L2.22 9.28a.75.75 0 011.06-1.06L6 10.94l6.72-6.72a.75.75 0 011.06 0z"></path></svg>
              <span class="flex-auto css-truncate-target" data-menu-button-text>minimum_bounding_geometry</span>
              <span class="flex-auto d-flex flex-justify-end">Function</span>
</a>        </div>
      </div>
      <footer class="SelectMenu-footer">
        <div class="d-flex flex-justify-between">
          Code navigation index up-to-date
          <svg class="octicon octicon-dot-fill text-green" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M8 4a4 4 0 100 8 4 4 0 000-8z"></path></svg>
        </div>
      </footer>
    </div>
  </details-menu>
</details>

      </h2>
      <a href="/VolpeUSDOT/FTOT/find/master"
            class="js-pjax-capture-input btn mr-2 d-none d-md-block"
            data-pjax
            data-hotkey="t">
        Go to file
      </a>

      <details id="blob-more-options-details" class="details-overlay details-reset position-relative">
  <summary role="button">
    <svg aria-label="More options" height="16" class="octicon octicon-kebab-horizontal" viewBox="0 0 16 16" version="1.1" width="16" role="img"><path d="M8 9a1.5 1.5 0 100-3 1.5 1.5 0 000 3zM1.5 9a1.5 1.5 0 100-3 1.5 1.5 0 000 3zm13 0a1.5 1.5 0 100-3 1.5 1.5 0 000 3z"></path></svg>
</summary>  <div>
    <ul class="dropdown-menu dropdown-menu-sw">
            <li class="d-block d-md-none">
              <a class="dropdown-item d-flex flex-items-baseline" data-hydro-click="{&quot;event_type&quot;:&quot;repository.click&quot;,&quot;payload&quot;:{&quot;target&quot;:&quot;FIND_FILE_BUTTON&quot;,&quot;repository_id&quot;:162637563,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}" data-hydro-click-hmac="1992fc7101dceebac842d1085bbcce06a78ce0f6d87e5b15bbe4bcb0d08421eb" data-ga-click="Repository, find file, location:repo overview" data-hotkey="t" data-pjax="true" href="/VolpeUSDOT/FTOT/find/master">
                <span class="flex-auto">Go to file</span>
                <span class="text-small text-gray" aria-hidden="true">T</span>
</a>            </li>
            <li data-toggle-for="blob-more-options-details">
              <button type="button" data-toggle-for="jumpto-line-details-dialog" class="btn-link dropdown-item">
                <span class="d-flex flex-items-baseline">
                  <span class="flex-auto">Go to line</span>
                  <span class="text-small text-gray" aria-hidden="true">L</span>
                </span>
              </button>
            </li>
            <li data-toggle-for="blob-more-options-details">
              <button type="button" data-toggle-for="jumpto-symbol-select-menu" class="btn-link dropdown-item">
                <span class="d-flex flex-items-baseline">
                  <span class="flex-auto">Go to definition</span>
                  <span class="text-small text-gray" aria-hidden="true">R</span>
                </span>
              </button>
            </li>
            <li class="dropdown-divider" role="none"></li>
            <li>
              <clipboard-copy value="program/ftot_routing.py" class="dropdown-item cursor-pointer" data-toggle-for="blob-more-options-details">
                Copy path
              </clipboard-copy>
            </li>
          </ul>
</div></details>    </div>



    <div class="Box d-flex flex-column flex-shrink-0 mb-3">
      
  <div class="Box-header Box-header--blue Details js-details-container">
      <div class="d-flex flex-items-center">
        <span class="flex-shrink-0 ml-n1 mr-n1 mt-n1 mb-n1">
          <a rel="contributor" data-skip-pjax="true" data-hovercard-type="user" data-hovercard-url="/users/aobergdc/hovercard" data-octo-click="hovercard-link-click" data-octo-dimensions="link_type:self" href="/aobergdc"><img class="avatar avatar-user" src="https://avatars2.githubusercontent.com/u/18051014?s=48&amp;v=4" width="24" height="24" alt="@aobergdc" /></a>
        </span>
        <div class="flex-1 d-flex flex-items-center ml-3 min-width-0">
          <div class="css-truncate css-truncate-overflow">
            <a class="text-bold link-gray-dark" rel="contributor" data-hovercard-type="user" data-hovercard-url="/users/aobergdc/hovercard" data-octo-click="hovercard-link-click" data-octo-dimensions="link_type:self" href="/aobergdc">aobergdc</a>

              <span>
                <a data-pjax="true" title="quick fix for an if/else statement which should just be an if statement" class="link-gray" href="/VolpeUSDOT/FTOT/commit/e9d292d2aa83a912bb0b1994503cee4ee40b4209">quick fix for an if/else statement which should just be an if statement</a>
              </span>
          </div>


          <span class="ml-2">
            <include-fragment accept="text/fragment+html" src="/VolpeUSDOT/FTOT/commit/e9d292d2aa83a912bb0b1994503cee4ee40b4209/rollup?direction=e" class="d-inline"></include-fragment>
          </span>
        </div>
        <div class="ml-3 d-flex flex-shrink-0 flex-items-center flex-justify-end text-gray no-wrap">
          <span class="d-none d-md-inline">
            <span>Latest commit</span>
            <a class="text-small text-mono link-gray" href="/VolpeUSDOT/FTOT/commit/e9d292d2aa83a912bb0b1994503cee4ee40b4209" data-pjax>e9d292d</a>
            <span itemprop="dateModified"><relative-time datetime="2020-10-13T12:46:35Z" class="no-wrap">Oct 13, 2020</relative-time></span>
          </span>

          <a data-pjax href="/VolpeUSDOT/FTOT/commits/master/program/ftot_routing.py" class="ml-3 no-wrap link-gray-dark no-underline">
            <svg text="gray" height="16" class="octicon octicon-history text-gray" viewBox="0 0 16 16" version="1.1" width="16" aria-hidden="true"><path fill-rule="evenodd" d="M1.643 3.143L.427 1.927A.25.25 0 000 2.104V5.75c0 .138.112.25.25.25h3.646a.25.25 0 00.177-.427L2.715 4.215a6.5 6.5 0 11-1.18 4.458.75.75 0 10-1.493.154 8.001 8.001 0 101.6-5.684zM7.75 4a.75.75 0 01.75.75v2.992l2.028.812a.75.75 0 01-.557 1.392l-2.5-1A.75.75 0 017 8.25v-3.5A.75.75 0 017.75 4z"></path></svg>
            <span class="d-none d-sm-inline">
              <strong>History</strong>
            </span>
          </a>
        </div>
      </div>

  </div>

  <div class="Box-body d-flex flex-items-center flex-auto border-bottom-0 flex-wrap" >
    <details class="details-reset details-overlay details-overlay-dark lh-default text-gray-dark float-left mr-3" id="blob_contributors_box">
      <summary class="link-gray-dark">
        <svg text="gray" height="16" class="octicon octicon-people text-gray" viewBox="0 0 16 16" version="1.1" width="16" aria-hidden="true"><path fill-rule="evenodd" d="M5.5 3.5a2 2 0 100 4 2 2 0 000-4zM2 5.5a3.5 3.5 0 115.898 2.549 5.507 5.507 0 013.034 4.084.75.75 0 11-1.482.235 4.001 4.001 0 00-7.9 0 .75.75 0 01-1.482-.236A5.507 5.507 0 013.102 8.05 3.49 3.49 0 012 5.5zM11 4a.75.75 0 100 1.5 1.5 1.5 0 01.666 2.844.75.75 0 00-.416.672v.352a.75.75 0 00.574.73c1.2.289 2.162 1.2 2.522 2.372a.75.75 0 101.434-.44 5.01 5.01 0 00-2.56-3.012A3 3 0 0011 4z"></path></svg>
        <strong>4</strong>
        
        contributors
      </summary>
      <details-dialog
        class="Box Box--overlay d-flex flex-column anim-fade-in fast"
        aria-label="Users who have contributed to this file"
        src="/VolpeUSDOT/FTOT/contributors-list/master/program/ftot_routing.py" preload>
        <div class="Box-header">
          <button class="Box-btn-octicon btn-octicon float-right" type="button" aria-label="Close dialog" data-close-dialog>
            <svg class="octicon octicon-x" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M3.72 3.72a.75.75 0 011.06 0L8 6.94l3.22-3.22a.75.75 0 111.06 1.06L9.06 8l3.22 3.22a.75.75 0 11-1.06 1.06L8 9.06l-3.22 3.22a.75.75 0 01-1.06-1.06L6.94 8 3.72 4.78a.75.75 0 010-1.06z"></path></svg>
          </button>
          <h3 class="Box-title">
            Users who have contributed to this file
          </h3>
        </div>
        <include-fragment class="octocat-spinner my-3" aria-label="Loading..."></include-fragment>
      </details-dialog>
    </details>
      <span class="">
    <a class="avatar-link" data-hovercard-type="user" data-hovercard-url="/users/matthewpearlson/hovercard" data-octo-click="hovercard-link-click" data-octo-dimensions="link_type:self" href="/VolpeUSDOT/FTOT/commits/master/program/ftot_routing.py?author=matthewpearlson">
      <img class="avatar mr-2 avatar-user" src="https://avatars0.githubusercontent.com/u/45362680?s=48&amp;v=4" width="24" height="24" alt="@matthewpearlson" /> 
</a>    <a class="avatar-link" data-hovercard-type="user" data-hovercard-url="/users/aobergdc/hovercard" data-octo-click="hovercard-link-click" data-octo-dimensions="link_type:self" href="/VolpeUSDOT/FTOT/commits/master/program/ftot_routing.py?author=aobergdc">
      <img class="avatar mr-2 avatar-user" src="https://avatars2.githubusercontent.com/u/18051014?s=48&amp;v=4" width="24" height="24" alt="@aobergdc" /> 
</a>    <a class="avatar-link" data-hovercard-type="user" data-hovercard-url="/users/ael-adle-VOLPE/hovercard" data-octo-click="hovercard-link-click" data-octo-dimensions="link_type:self" href="/VolpeUSDOT/FTOT/commits/master/program/ftot_routing.py?author=ael-adle-VOLPE">
      <img class="avatar mr-2 avatar-user" src="https://avatars0.githubusercontent.com/u/66736549?s=48&amp;v=4" width="24" height="24" alt="@ael-adle-VOLPE" /> 
</a>    <a class="avatar-link" data-hovercard-type="user" data-hovercard-url="/users/kirbyledvina/hovercard" data-octo-click="hovercard-link-click" data-octo-dimensions="link_type:self" href="/VolpeUSDOT/FTOT/commits/master/program/ftot_routing.py?author=kirbyledvina">
      <img class="avatar mr-2 avatar-user" src="https://avatars3.githubusercontent.com/u/65978091?s=48&amp;v=4" width="24" height="24" alt="@kirbyledvina" /> 
</a>
</span>

  </div>
    </div>






    <div class="Box mt-3 position-relative
      ">
      
<div class="Box-header py-2 d-flex flex-column flex-shrink-0 flex-md-row flex-md-items-center">
  <div class="text-mono f6 flex-auto pr-3 flex-order-2 flex-md-order-1 mt-2 mt-md-0">

      1150 lines (890 sloc)
      <span class="file-info-divider"></span>
    54.2 KB
  </div>

  <div class="d-flex py-1 py-md-0 flex-auto flex-order-1 flex-md-order-2 flex-sm-grow-0 flex-justify-between">

    <div class="BtnGroup">
      <a href="/VolpeUSDOT/FTOT/raw/master/program/ftot_routing.py" id="raw-url" role="button" class="btn btn-sm BtnGroup-item ">Raw</a>
        <a href="/VolpeUSDOT/FTOT/blame/master/program/ftot_routing.py" data-hotkey="b" role="button" class="btn js-update-url-with-hash btn-sm BtnGroup-item ">Blame</a>
    </div>

    <div>
          <a class="btn-octicon tooltipped tooltipped-nw js-remove-unless-platform"
             data-platforms="windows,mac"
             href="https://desktop.github.com"
             aria-label="Open this file in GitHub Desktop"
             data-ga-click="Repository, open with desktop">
              <svg class="octicon octicon-device-desktop" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M1.75 2.5h12.5a.25.25 0 01.25.25v7.5a.25.25 0 01-.25.25H1.75a.25.25 0 01-.25-.25v-7.5a.25.25 0 01.25-.25zM14.25 1H1.75A1.75 1.75 0 000 2.75v7.5C0 11.216.784 12 1.75 12h3.727c-.1 1.041-.52 1.872-1.292 2.757A.75.75 0 004.75 16h6.5a.75.75 0 00.565-1.243c-.772-.885-1.193-1.716-1.292-2.757h3.727A1.75 1.75 0 0016 10.25v-7.5A1.75 1.75 0 0014.25 1zM9.018 12H6.982a5.72 5.72 0 01-.765 2.5h3.566a5.72 5.72 0 01-.765-2.5z"></path></svg>
          </a>

          <!-- '"` --><!-- </textarea></xmp> --></option></form><form class="inline-form js-update-url-with-hash" action="/VolpeUSDOT/FTOT/edit/master/program/ftot_routing.py" accept-charset="UTF-8" method="post"><input type="hidden" name="authenticity_token" value="8hlbrKdNCrkMRMVtYv/5oMpPQYNBWYLJJcHW5L9BZjtqMtVa/MurWctqTlx/vKmS0q9plFsJ5aLYI1Etj1AQuA==" />
            <button class="btn-octicon tooltipped tooltipped-nw" type="submit"
              aria-label="Edit this file" data-hotkey="e" data-disable-with>
              <svg class="octicon octicon-pencil" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M11.013 1.427a1.75 1.75 0 012.474 0l1.086 1.086a1.75 1.75 0 010 2.474l-8.61 8.61c-.21.21-.47.364-.756.445l-3.251.93a.75.75 0 01-.927-.928l.929-3.25a1.75 1.75 0 01.445-.758l8.61-8.61zm1.414 1.06a.25.25 0 00-.354 0L10.811 3.75l1.439 1.44 1.263-1.263a.25.25 0 000-.354l-1.086-1.086zM11.189 6.25L9.75 4.81l-6.286 6.287a.25.25 0 00-.064.108l-.558 1.953 1.953-.558a.249.249 0 00.108-.064l6.286-6.286z"></path></svg>
            </button>
</form>
          <!-- '"` --><!-- </textarea></xmp> --></option></form><form class="inline-form" action="/VolpeUSDOT/FTOT/delete/master/program/ftot_routing.py" accept-charset="UTF-8" method="post"><input type="hidden" name="authenticity_token" value="4pg8NAnF2FX8rSiK0Ohwl+9AzgQGBo5zfP2Wj15F/ApTcZDJdTb3r2bQu3i7HIOuNt0CifhI6KU8RFvl4s82jg==" />
            <button class="btn-octicon btn-octicon-danger tooltipped tooltipped-nw" type="submit"
              aria-label="Delete this file" data-disable-with>
              <svg class="octicon octicon-trashcan" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M6.5 1.75a.25.25 0 01.25-.25h2.5a.25.25 0 01.25.25V3h-3V1.75zm4.5 0V3h2.25a.75.75 0 010 1.5H2.75a.75.75 0 010-1.5H5V1.75C5 .784 5.784 0 6.75 0h2.5C10.216 0 11 .784 11 1.75zM4.496 6.675a.75.75 0 10-1.492.15l.66 6.6A1.75 1.75 0 005.405 15h5.19c.9 0 1.652-.681 1.741-1.576l.66-6.6a.75.75 0 00-1.492-.149l-.66 6.6a.25.25 0 01-.249.225h-5.19a.25.25 0 01-.249-.225l-.66-6.6z"></path></svg>
            </button>
</form>    </div>
  </div>
</div>

      

  <div itemprop="text" class="Box-body p-0 blob-wrapper data type-python  gist-border-0">
      
<table class="highlight tab-size js-file-line-container" data-tab-size="8" data-paste-markdown-skip>
      <tr>
        <td id="L1" class="blob-num js-line-number" data-line-number="1"></td>
        <td id="LC1" class="blob-code blob-code-inner js-file-line"><span class=pl-c># ------------------------------------------------------------------------------</span></td>
      </tr>
      <tr>
        <td id="L2" class="blob-num js-line-number" data-line-number="2"></td>
        <td id="LC2" class="blob-code blob-code-inner js-file-line"><span class=pl-c># ftot_routing.py</span></td>
      </tr>
      <tr>
        <td id="L3" class="blob-num js-line-number" data-line-number="3"></td>
        <td id="LC3" class="blob-code blob-code-inner js-file-line"><span class=pl-c># Purpose: the purpose of this module is to clean up,</span></td>
      </tr>
      <tr>
        <td id="L4" class="blob-num js-line-number" data-line-number="4"></td>
        <td id="LC4" class="blob-code blob-code-inner js-file-line"><span class=pl-c># create the locations_fc,</span></td>
      </tr>
      <tr>
        <td id="L5" class="blob-num js-line-number" data-line-number="5"></td>
        <td id="LC5" class="blob-code blob-code-inner js-file-line"><span class=pl-c># hook locations into the network,</span></td>
      </tr>
      <tr>
        <td id="L6" class="blob-num js-line-number" data-line-number="6"></td>
        <td id="LC6" class="blob-code blob-code-inner js-file-line"><span class=pl-c># ignore locations not connected to the network,</span></td>
      </tr>
      <tr>
        <td id="L7" class="blob-num js-line-number" data-line-number="7"></td>
        <td id="LC7" class="blob-code blob-code-inner js-file-line"><span class=pl-c># export capacity information to the main.db,</span></td>
      </tr>
      <tr>
        <td id="L8" class="blob-num js-line-number" data-line-number="8"></td>
        <td id="LC8" class="blob-code blob-code-inner js-file-line"><span class=pl-c># export the assets from GIS export_fcs_from_main_gdb</span></td>
      </tr>
      <tr>
        <td id="L9" class="blob-num js-line-number" data-line-number="9"></td>
        <td id="LC9" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L10" class="blob-num js-line-number" data-line-number="10"></td>
        <td id="LC10" class="blob-code blob-code-inner js-file-line"><span class=pl-c># Revised: 1/15/19 - MNP</span></td>
      </tr>
      <tr>
        <td id="L11" class="blob-num js-line-number" data-line-number="11"></td>
        <td id="LC11" class="blob-code blob-code-inner js-file-line"><span class=pl-c># ------------------------------------------------------------------------------</span></td>
      </tr>
      <tr>
        <td id="L12" class="blob-num js-line-number" data-line-number="12"></td>
        <td id="LC12" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L13" class="blob-num js-line-number" data-line-number="13"></td>
        <td id="LC13" class="blob-code blob-code-inner js-file-line"><span class=pl-k>import</span> <span class=pl-s1>os</span></td>
      </tr>
      <tr>
        <td id="L14" class="blob-num js-line-number" data-line-number="14"></td>
        <td id="LC14" class="blob-code blob-code-inner js-file-line"><span class=pl-k>import</span> <span class=pl-s1>arcpy</span></td>
      </tr>
      <tr>
        <td id="L15" class="blob-num js-line-number" data-line-number="15"></td>
        <td id="LC15" class="blob-code blob-code-inner js-file-line"><span class=pl-k>import</span> <span class=pl-s1>sqlite3</span></td>
      </tr>
      <tr>
        <td id="L16" class="blob-num js-line-number" data-line-number="16"></td>
        <td id="LC16" class="blob-code blob-code-inner js-file-line"><span class=pl-k>import</span> <span class=pl-s1>ftot_supporting_gis</span></td>
      </tr>
      <tr>
        <td id="L17" class="blob-num js-line-number" data-line-number="17"></td>
        <td id="LC17" class="blob-code blob-code-inner js-file-line"><span class=pl-k>from</span> <span class=pl-s1>ftot</span> <span class=pl-k>import</span> <span class=pl-v>Q_</span></td>
      </tr>
      <tr>
        <td id="L18" class="blob-num js-line-number" data-line-number="18"></td>
        <td id="LC18" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L19" class="blob-num js-line-number" data-line-number="19"></td>
        <td id="LC19" class="blob-code blob-code-inner js-file-line"><span class=pl-v>LCC_PROJ</span> <span class=pl-c1>=</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>SpatialReference</span>(<span class=pl-s>&#39;USA Contiguous Lambert Conformal Conic&#39;</span>)</td>
      </tr>
      <tr>
        <td id="L20" class="blob-num js-line-number" data-line-number="20"></td>
        <td id="LC20" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L21" class="blob-num js-line-number" data-line-number="21"></td>
        <td id="LC21" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L22" class="blob-num js-line-number" data-line-number="22"></td>
        <td id="LC22" class="blob-code blob-code-inner js-file-line"><span class=pl-c># ========================================================================</span></td>
      </tr>
      <tr>
        <td id="L23" class="blob-num js-line-number" data-line-number="23"></td>
        <td id="LC23" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L24" class="blob-num js-line-number" data-line-number="24"></td>
        <td id="LC24" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L25" class="blob-num js-line-number" data-line-number="25"></td>
        <td id="LC25" class="blob-code blob-code-inner js-file-line"><span class=pl-k>def</span> <span class=pl-en>connectivity</span>(<span class=pl-s1>the_scenario</span>, <span class=pl-s1>logger</span>):</td>
      </tr>
      <tr>
        <td id="L26" class="blob-num js-line-number" data-line-number="26"></td>
        <td id="LC26" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L27" class="blob-num js-line-number" data-line-number="27"></td>
        <td id="LC27" class="blob-code blob-code-inner js-file-line">        <span class=pl-en>checks_and_cleanup</span>(<span class=pl-s1>the_scenario</span>, <span class=pl-s1>logger</span>)</td>
      </tr>
      <tr>
        <td id="L28" class="blob-num js-line-number" data-line-number="28"></td>
        <td id="LC28" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L29" class="blob-num js-line-number" data-line-number="29"></td>
        <td id="LC29" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># create the locations_fc</span></td>
      </tr>
      <tr>
        <td id="L30" class="blob-num js-line-number" data-line-number="30"></td>
        <td id="LC30" class="blob-code blob-code-inner js-file-line">        <span class=pl-en>create_locations_fc</span>(<span class=pl-s1>the_scenario</span>, <span class=pl-s1>logger</span>)</td>
      </tr>
      <tr>
        <td id="L31" class="blob-num js-line-number" data-line-number="31"></td>
        <td id="LC31" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L32" class="blob-num js-line-number" data-line-number="32"></td>
        <td id="LC32" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># use MBG to subset the road network to a buffer around the locations FC</span></td>
      </tr>
      <tr>
        <td id="L33" class="blob-num js-line-number" data-line-number="33"></td>
        <td id="LC33" class="blob-code blob-code-inner js-file-line">        <span class=pl-en>minimum_bounding_geometry</span>(<span class=pl-s1>the_scenario</span>, <span class=pl-s1>logger</span>)</td>
      </tr>
      <tr>
        <td id="L34" class="blob-num js-line-number" data-line-number="34"></td>
        <td id="LC34" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L35" class="blob-num js-line-number" data-line-number="35"></td>
        <td id="LC35" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># hook locations into the network</span></td>
      </tr>
      <tr>
        <td id="L36" class="blob-num js-line-number" data-line-number="36"></td>
        <td id="LC36" class="blob-code blob-code-inner js-file-line">        <span class=pl-en>hook_locations_into_network</span>(<span class=pl-s1>the_scenario</span>, <span class=pl-s1>logger</span>)</td>
      </tr>
      <tr>
        <td id="L37" class="blob-num js-line-number" data-line-number="37"></td>
        <td id="LC37" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L38" class="blob-num js-line-number" data-line-number="38"></td>
        <td id="LC38" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># ignore locations not connected to the network</span></td>
      </tr>
      <tr>
        <td id="L39" class="blob-num js-line-number" data-line-number="39"></td>
        <td id="LC39" class="blob-code blob-code-inner js-file-line">        <span class=pl-en>ignore_locations_not_connected_to_network</span>(<span class=pl-s1>the_scenario</span>, <span class=pl-s1>logger</span>)</td>
      </tr>
      <tr>
        <td id="L40" class="blob-num js-line-number" data-line-number="40"></td>
        <td id="LC40" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L41" class="blob-num js-line-number" data-line-number="41"></td>
        <td id="LC41" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># report out material missing after connecting to the network</span></td>
      </tr>
      <tr>
        <td id="L42" class="blob-num js-line-number" data-line-number="42"></td>
        <td id="LC42" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>from</span> <span class=pl-s1>ftot_facilities</span> <span class=pl-k>import</span> <span class=pl-s1>db_report_commodity_potentials</span></td>
      </tr>
      <tr>
        <td id="L43" class="blob-num js-line-number" data-line-number="43"></td>
        <td id="LC43" class="blob-code blob-code-inner js-file-line">        <span class=pl-en>db_report_commodity_potentials</span>(<span class=pl-s1>the_scenario</span>, <span class=pl-s1>logger</span>)</td>
      </tr>
      <tr>
        <td id="L44" class="blob-num js-line-number" data-line-number="44"></td>
        <td id="LC44" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L45" class="blob-num js-line-number" data-line-number="45"></td>
        <td id="LC45" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># export capacity information to the main.db</span></td>
      </tr>
      <tr>
        <td id="L46" class="blob-num js-line-number" data-line-number="46"></td>
        <td id="LC46" class="blob-code blob-code-inner js-file-line">        <span class=pl-en>cache_capacity_information</span>(<span class=pl-s1>the_scenario</span>, <span class=pl-s1>logger</span>)</td>
      </tr>
      <tr>
        <td id="L47" class="blob-num js-line-number" data-line-number="47"></td>
        <td id="LC47" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L48" class="blob-num js-line-number" data-line-number="48"></td>
        <td id="LC48" class="blob-code blob-code-inner js-file-line"><span class=pl-c># =========================================================================</span></td>
      </tr>
      <tr>
        <td id="L49" class="blob-num js-line-number" data-line-number="49"></td>
        <td id="LC49" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L50" class="blob-num js-line-number" data-line-number="50"></td>
        <td id="LC50" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L51" class="blob-num js-line-number" data-line-number="51"></td>
        <td id="LC51" class="blob-code blob-code-inner js-file-line"><span class=pl-k>def</span> <span class=pl-en>checks_and_cleanup</span>(<span class=pl-s1>the_scenario</span>, <span class=pl-s1>logger</span>):</td>
      </tr>
      <tr>
        <td id="L52" class="blob-num js-line-number" data-line-number="52"></td>
        <td id="LC52" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>info</span>(<span class=pl-s>&quot;start: checks_and_cleanup&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L53" class="blob-num js-line-number" data-line-number="53"></td>
        <td id="LC53" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;delete the locations_fc from the main.gdb&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L54" class="blob-num js-line-number" data-line-number="54"></td>
        <td id="LC54" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L55" class="blob-num js-line-number" data-line-number="55"></td>
        <td id="LC55" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>scenario_gdb</span> <span class=pl-c1>=</span> <span class=pl-s1>the_scenario</span>.<span class=pl-s1>main_gdb</span></td>
      </tr>
      <tr>
        <td id="L56" class="blob-num js-line-number" data-line-number="56"></td>
        <td id="LC56" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L57" class="blob-num js-line-number" data-line-number="57"></td>
        <td id="LC57" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-c1>not</span> <span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>exists</span>(<span class=pl-s1>scenario_gdb</span>):</td>
      </tr>
      <tr>
        <td id="L58" class="blob-num js-line-number" data-line-number="58"></td>
        <td id="LC58" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>error</span> <span class=pl-c1>=</span> <span class=pl-s>&quot;can&#39;t find scenario gdb {}&quot;</span>.<span class=pl-en>format</span>(<span class=pl-s1>scenario_gdb</span>)</td>
      </tr>
      <tr>
        <td id="L59" class="blob-num js-line-number" data-line-number="59"></td>
        <td id="LC59" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>raise</span> <span class=pl-v>IOError</span>(<span class=pl-s1>error</span>)</td>
      </tr>
      <tr>
        <td id="L60" class="blob-num js-line-number" data-line-number="60"></td>
        <td id="LC60" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L61" class="blob-num js-line-number" data-line-number="61"></td>
        <td id="LC61" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># delete the old location FC before we create it.</span></td>
      </tr>
      <tr>
        <td id="L62" class="blob-num js-line-number" data-line-number="62"></td>
        <td id="LC62" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>Exists</span>(<span class=pl-s1>the_scenario</span>.<span class=pl-s1>locations_fc</span>):</td>
      </tr>
      <tr>
        <td id="L63" class="blob-num js-line-number" data-line-number="63"></td>
        <td id="LC63" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;deleting locations_fc: {}&quot;</span>.<span class=pl-en>format</span>(<span class=pl-s1>the_scenario</span>.<span class=pl-s1>locations_fc</span>))</td>
      </tr>
      <tr>
        <td id="L64" class="blob-num js-line-number" data-line-number="64"></td>
        <td id="LC64" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s1>the_scenario</span>.<span class=pl-s1>locations_fc</span>)</td>
      </tr>
      <tr>
        <td id="L65" class="blob-num js-line-number" data-line-number="65"></td>
        <td id="LC65" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L66" class="blob-num js-line-number" data-line-number="66"></td>
        <td id="LC66" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># check for scenario DB</span></td>
      </tr>
      <tr>
        <td id="L67" class="blob-num js-line-number" data-line-number="67"></td>
        <td id="LC67" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># ---------------------------------</span></td>
      </tr>
      <tr>
        <td id="L68" class="blob-num js-line-number" data-line-number="68"></td>
        <td id="LC68" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>scenario_db</span> <span class=pl-c1>=</span> <span class=pl-s1>the_scenario</span>.<span class=pl-s1>main_db</span></td>
      </tr>
      <tr>
        <td id="L69" class="blob-num js-line-number" data-line-number="69"></td>
        <td id="LC69" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-c1>not</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>Exists</span>(<span class=pl-s1>scenario_db</span>):</td>
      </tr>
      <tr>
        <td id="L70" class="blob-num js-line-number" data-line-number="70"></td>
        <td id="LC70" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>raise</span> <span class=pl-v>Exception</span>(<span class=pl-s>&quot;scenario_db not found {} &quot;</span>.<span class=pl-en>format</span>(<span class=pl-s1>scenario_db</span>))</td>
      </tr>
      <tr>
        <td id="L71" class="blob-num js-line-number" data-line-number="71"></td>
        <td id="LC71" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L72" class="blob-num js-line-number" data-line-number="72"></td>
        <td id="LC72" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;finish: checks_and_cleanup&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L73" class="blob-num js-line-number" data-line-number="73"></td>
        <td id="LC73" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L74" class="blob-num js-line-number" data-line-number="74"></td>
        <td id="LC74" class="blob-code blob-code-inner js-file-line"><span class=pl-c># ==============================================================================</span></td>
      </tr>
      <tr>
        <td id="L75" class="blob-num js-line-number" data-line-number="75"></td>
        <td id="LC75" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L76" class="blob-num js-line-number" data-line-number="76"></td>
        <td id="LC76" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L77" class="blob-num js-line-number" data-line-number="77"></td>
        <td id="LC77" class="blob-code blob-code-inner js-file-line"><span class=pl-k>def</span> <span class=pl-en>create_locations_fc</span>(<span class=pl-s1>the_scenario</span>, <span class=pl-s1>logger</span>):</td>
      </tr>
      <tr>
        <td id="L78" class="blob-num js-line-number" data-line-number="78"></td>
        <td id="LC78" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>info</span>(<span class=pl-s>&quot;start: create_locations_fc&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L79" class="blob-num js-line-number" data-line-number="79"></td>
        <td id="LC79" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>co_location_offet</span> <span class=pl-c1>=</span> <span class=pl-c1>0.1</span></td>
      </tr>
      <tr>
        <td id="L80" class="blob-num js-line-number" data-line-number="80"></td>
        <td id="LC80" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;co-location offset is necessary to prevent the locations from being treated as intermodal &quot;</span></td>
      </tr>
      <tr>
        <td id="L81" class="blob-num js-line-number" data-line-number="81"></td>
        <td id="LC81" class="blob-code blob-code-inner js-file-line">                 <span class=pl-s>&quot;facilities.&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L82" class="blob-num js-line-number" data-line-number="82"></td>
        <td id="LC82" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;collocation off-set: {} meters&quot;</span>.<span class=pl-en>format</span>(<span class=pl-s1>co_location_offet</span>))</td>
      </tr>
      <tr>
        <td id="L83" class="blob-num js-line-number" data-line-number="83"></td>
        <td id="LC83" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L84" class="blob-num js-line-number" data-line-number="84"></td>
        <td id="LC84" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>locations_fc</span> <span class=pl-c1>=</span> <span class=pl-s1>the_scenario</span>.<span class=pl-s1>locations_fc</span></td>
      </tr>
      <tr>
        <td id="L85" class="blob-num js-line-number" data-line-number="85"></td>
        <td id="LC85" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L86" class="blob-num js-line-number" data-line-number="86"></td>
        <td id="LC86" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># clear out the location FC before we create it.</span></td>
      </tr>
      <tr>
        <td id="L87" class="blob-num js-line-number" data-line-number="87"></td>
        <td id="LC87" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>from</span> <span class=pl-s1>ftot_facilities</span> <span class=pl-k>import</span> <span class=pl-s1>gis_clear_feature_class</span></td>
      </tr>
      <tr>
        <td id="L88" class="blob-num js-line-number" data-line-number="88"></td>
        <td id="LC88" class="blob-code blob-code-inner js-file-line">    <span class=pl-en>gis_clear_feature_class</span>(<span class=pl-s1>locations_fc</span>, <span class=pl-s1>logger</span>)</td>
      </tr>
      <tr>
        <td id="L89" class="blob-num js-line-number" data-line-number="89"></td>
        <td id="LC89" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L90" class="blob-num js-line-number" data-line-number="90"></td>
        <td id="LC90" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># create the featureclass</span></td>
      </tr>
      <tr>
        <td id="L91" class="blob-num js-line-number" data-line-number="91"></td>
        <td id="LC91" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>CreateFeatureclass_management</span>(<span class=pl-s1>the_scenario</span>.<span class=pl-s1>main_gdb</span>, <span class=pl-s>&quot;locations&quot;</span>, <span class=pl-s>&quot;POINT&quot;</span>, <span class=pl-s>&quot;#&quot;</span>, <span class=pl-s>&quot;DISABLED&quot;</span>, <span class=pl-s>&quot;DISABLED&quot;</span>,</td>
      </tr>
      <tr>
        <td id="L92" class="blob-num js-line-number" data-line-number="92"></td>
        <td id="LC92" class="blob-code blob-code-inner js-file-line">                                        <span class=pl-s1>ftot_supporting_gis</span>.<span class=pl-v>LCC_PROJ</span>)</td>
      </tr>
      <tr>
        <td id="L93" class="blob-num js-line-number" data-line-number="93"></td>
        <td id="LC93" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L94" class="blob-num js-line-number" data-line-number="94"></td>
        <td id="LC94" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># add the location_id field</span></td>
      </tr>
      <tr>
        <td id="L95" class="blob-num js-line-number" data-line-number="95"></td>
        <td id="LC95" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s1>locations_fc</span>, <span class=pl-s>&quot;location_id&quot;</span>, <span class=pl-s>&quot;TEXT&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L96" class="blob-num js-line-number" data-line-number="96"></td>
        <td id="LC96" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L97" class="blob-num js-line-number" data-line-number="97"></td>
        <td id="LC97" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># add the location_id_name field</span></td>
      </tr>
      <tr>
        <td id="L98" class="blob-num js-line-number" data-line-number="98"></td>
        <td id="LC98" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s1>locations_fc</span>, <span class=pl-s>&quot;location_id_name&quot;</span>, <span class=pl-s>&quot;TEXT&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L99" class="blob-num js-line-number" data-line-number="99"></td>
        <td id="LC99" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L100" class="blob-num js-line-number" data-line-number="100"></td>
        <td id="LC100" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># add the connects_road field</span></td>
      </tr>
      <tr>
        <td id="L101" class="blob-num js-line-number" data-line-number="101"></td>
        <td id="LC101" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s1>locations_fc</span>, <span class=pl-s>&quot;connects_road&quot;</span>, <span class=pl-s>&quot;SHORT&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L102" class="blob-num js-line-number" data-line-number="102"></td>
        <td id="LC102" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L103" class="blob-num js-line-number" data-line-number="103"></td>
        <td id="LC103" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># add the connects_rail field</span></td>
      </tr>
      <tr>
        <td id="L104" class="blob-num js-line-number" data-line-number="104"></td>
        <td id="LC104" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s1>locations_fc</span>, <span class=pl-s>&quot;connects_rail&quot;</span>, <span class=pl-s>&quot;SHORT&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L105" class="blob-num js-line-number" data-line-number="105"></td>
        <td id="LC105" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L106" class="blob-num js-line-number" data-line-number="106"></td>
        <td id="LC106" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># add the connects_water field</span></td>
      </tr>
      <tr>
        <td id="L107" class="blob-num js-line-number" data-line-number="107"></td>
        <td id="LC107" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s1>locations_fc</span>, <span class=pl-s>&quot;connects_water&quot;</span>, <span class=pl-s>&quot;SHORT&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L108" class="blob-num js-line-number" data-line-number="108"></td>
        <td id="LC108" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L109" class="blob-num js-line-number" data-line-number="109"></td>
        <td id="LC109" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># add the connects_pipeline_prod field</span></td>
      </tr>
      <tr>
        <td id="L110" class="blob-num js-line-number" data-line-number="110"></td>
        <td id="LC110" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s1>locations_fc</span>, <span class=pl-s>&quot;connects_pipeline_prod_trf_rts&quot;</span>, <span class=pl-s>&quot;SHORT&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L111" class="blob-num js-line-number" data-line-number="111"></td>
        <td id="LC111" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L112" class="blob-num js-line-number" data-line-number="112"></td>
        <td id="LC112" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># add the connects_pipeline_crude field</span></td>
      </tr>
      <tr>
        <td id="L113" class="blob-num js-line-number" data-line-number="113"></td>
        <td id="LC113" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s1>locations_fc</span>, <span class=pl-s>&quot;connects_pipeline_crude_trf_rts&quot;</span>, <span class=pl-s>&quot;SHORT&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L114" class="blob-num js-line-number" data-line-number="114"></td>
        <td id="LC114" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L115" class="blob-num js-line-number" data-line-number="115"></td>
        <td id="LC115" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># add the ignore field</span></td>
      </tr>
      <tr>
        <td id="L116" class="blob-num js-line-number" data-line-number="116"></td>
        <td id="LC116" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s1>locations_fc</span>, <span class=pl-s>&quot;ignore&quot;</span>, <span class=pl-s>&quot;SHORT&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L117" class="blob-num js-line-number" data-line-number="117"></td>
        <td id="LC117" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L118" class="blob-num js-line-number" data-line-number="118"></td>
        <td id="LC118" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># start an edit session for the insert cursor</span></td>
      </tr>
      <tr>
        <td id="L119" class="blob-num js-line-number" data-line-number="119"></td>
        <td id="LC119" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span> <span class=pl-c1>=</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>Editor</span>(<span class=pl-s1>the_scenario</span>.<span class=pl-s1>main_gdb</span>)</td>
      </tr>
      <tr>
        <td id="L120" class="blob-num js-line-number" data-line-number="120"></td>
        <td id="LC120" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span>.<span class=pl-en>startEditing</span>(<span class=pl-c1>False</span>, <span class=pl-c1>False</span>)</td>
      </tr>
      <tr>
        <td id="L121" class="blob-num js-line-number" data-line-number="121"></td>
        <td id="LC121" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span>.<span class=pl-en>startOperation</span>()</td>
      </tr>
      <tr>
        <td id="L122" class="blob-num js-line-number" data-line-number="122"></td>
        <td id="LC122" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L123" class="blob-num js-line-number" data-line-number="123"></td>
        <td id="LC123" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># create insert  cursor</span></td>
      </tr>
      <tr>
        <td id="L124" class="blob-num js-line-number" data-line-number="124"></td>
        <td id="LC124" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>with</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>InsertCursor</span>(<span class=pl-s1>locations_fc</span>, [<span class=pl-s>&quot;location_id_name&quot;</span>, <span class=pl-s>&quot;location_id&quot;</span>, <span class=pl-s>&quot;SHAPE@&quot;</span>]) <span class=pl-k>as</span> <span class=pl-s1>insert_cursor</span>:</td>
      </tr>
      <tr>
        <td id="L125" class="blob-num js-line-number" data-line-number="125"></td>
        <td id="LC125" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L126" class="blob-num js-line-number" data-line-number="126"></td>
        <td id="LC126" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># loop through DB and populate the fc</span></td>
      </tr>
      <tr>
        <td id="L127" class="blob-num js-line-number" data-line-number="127"></td>
        <td id="LC127" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>with</span> <span class=pl-s1>sqlite3</span>.<span class=pl-en>connect</span>(<span class=pl-s1>the_scenario</span>.<span class=pl-s1>main_db</span>) <span class=pl-k>as</span> <span class=pl-s1>db_con</span>:</td>
      </tr>
      <tr>
        <td id="L128" class="blob-num js-line-number" data-line-number="128"></td>
        <td id="LC128" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L129" class="blob-num js-line-number" data-line-number="129"></td>
        <td id="LC129" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>sql</span> <span class=pl-c1>=</span> <span class=pl-s>&quot;select * from locations;&quot;</span></td>
      </tr>
      <tr>
        <td id="L130" class="blob-num js-line-number" data-line-number="130"></td>
        <td id="LC130" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>db_cur</span> <span class=pl-c1>=</span> <span class=pl-s1>db_con</span>.<span class=pl-en>execute</span>(<span class=pl-s1>sql</span>)</td>
      </tr>
      <tr>
        <td id="L131" class="blob-num js-line-number" data-line-number="131"></td>
        <td id="LC131" class="blob-code blob-code-inner js-file-line">            <span class=pl-k>for</span> <span class=pl-s1>row</span> <span class=pl-c1>in</span> <span class=pl-s1>db_cur</span>:</td>
      </tr>
      <tr>
        <td id="L132" class="blob-num js-line-number" data-line-number="132"></td>
        <td id="LC132" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>location_id</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>0</span>]</td>
      </tr>
      <tr>
        <td id="L133" class="blob-num js-line-number" data-line-number="133"></td>
        <td id="LC133" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L134" class="blob-num js-line-number" data-line-number="134"></td>
        <td id="LC134" class="blob-code blob-code-inner js-file-line">                <span class=pl-c># create a point for each location &quot;in&quot;</span></td>
      </tr>
      <tr>
        <td id="L135" class="blob-num js-line-number" data-line-number="135"></td>
        <td id="LC135" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>location_point</span> <span class=pl-c1>=</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>Point</span>()</td>
      </tr>
      <tr>
        <td id="L136" class="blob-num js-line-number" data-line-number="136"></td>
        <td id="LC136" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>location_point</span>.<span class=pl-v>X</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>1</span>] <span class=pl-c1>+</span> <span class=pl-s1>co_location_offet</span></td>
      </tr>
      <tr>
        <td id="L137" class="blob-num js-line-number" data-line-number="137"></td>
        <td id="LC137" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>location_point</span>.<span class=pl-v>Y</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>2</span>] <span class=pl-c1>+</span> <span class=pl-s1>co_location_offet</span></td>
      </tr>
      <tr>
        <td id="L138" class="blob-num js-line-number" data-line-number="138"></td>
        <td id="LC138" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>location_point_geom</span> <span class=pl-c1>=</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>PointGeometry</span>(<span class=pl-s1>location_point</span>, <span class=pl-v>LCC_PROJ</span>)</td>
      </tr>
      <tr>
        <td id="L139" class="blob-num js-line-number" data-line-number="139"></td>
        <td id="LC139" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L140" class="blob-num js-line-number" data-line-number="140"></td>
        <td id="LC140" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>insert_cursor</span>.<span class=pl-en>insertRow</span>([<span class=pl-en>str</span>(<span class=pl-s1>location_id</span>) <span class=pl-c1>+</span> <span class=pl-s>&quot;_OUT&quot;</span>, <span class=pl-s1>location_id</span>, <span class=pl-s1>location_point_geom</span>])</td>
      </tr>
      <tr>
        <td id="L141" class="blob-num js-line-number" data-line-number="141"></td>
        <td id="LC141" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L142" class="blob-num js-line-number" data-line-number="142"></td>
        <td id="LC142" class="blob-code blob-code-inner js-file-line">                <span class=pl-c># create a point for each location &quot;out&quot;</span></td>
      </tr>
      <tr>
        <td id="L143" class="blob-num js-line-number" data-line-number="143"></td>
        <td id="LC143" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>location_point</span> <span class=pl-c1>=</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>Point</span>()</td>
      </tr>
      <tr>
        <td id="L144" class="blob-num js-line-number" data-line-number="144"></td>
        <td id="LC144" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>location_point</span>.<span class=pl-v>X</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>1</span>] <span class=pl-c1>-</span> <span class=pl-s1>co_location_offet</span></td>
      </tr>
      <tr>
        <td id="L145" class="blob-num js-line-number" data-line-number="145"></td>
        <td id="LC145" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>location_point</span>.<span class=pl-v>Y</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>2</span>] <span class=pl-c1>-</span> <span class=pl-s1>co_location_offet</span></td>
      </tr>
      <tr>
        <td id="L146" class="blob-num js-line-number" data-line-number="146"></td>
        <td id="LC146" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>location_point_geom</span> <span class=pl-c1>=</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>PointGeometry</span>(<span class=pl-s1>location_point</span>, <span class=pl-v>LCC_PROJ</span>)</td>
      </tr>
      <tr>
        <td id="L147" class="blob-num js-line-number" data-line-number="147"></td>
        <td id="LC147" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L148" class="blob-num js-line-number" data-line-number="148"></td>
        <td id="LC148" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>insert_cursor</span>.<span class=pl-en>insertRow</span>([<span class=pl-en>str</span>(<span class=pl-s1>location_id</span>) <span class=pl-c1>+</span> <span class=pl-s>&quot;_IN&quot;</span>, <span class=pl-s1>location_id</span>, <span class=pl-s1>location_point_geom</span>])</td>
      </tr>
      <tr>
        <td id="L149" class="blob-num js-line-number" data-line-number="149"></td>
        <td id="LC149" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L150" class="blob-num js-line-number" data-line-number="150"></td>
        <td id="LC150" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span>.<span class=pl-en>stopOperation</span>()</td>
      </tr>
      <tr>
        <td id="L151" class="blob-num js-line-number" data-line-number="151"></td>
        <td id="LC151" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span>.<span class=pl-en>stopEditing</span>(<span class=pl-c1>True</span>)</td>
      </tr>
      <tr>
        <td id="L152" class="blob-num js-line-number" data-line-number="152"></td>
        <td id="LC152" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L153" class="blob-num js-line-number" data-line-number="153"></td>
        <td id="LC153" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;finish: create_locations_fc&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L154" class="blob-num js-line-number" data-line-number="154"></td>
        <td id="LC154" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L155" class="blob-num js-line-number" data-line-number="155"></td>
        <td id="LC155" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L156" class="blob-num js-line-number" data-line-number="156"></td>
        <td id="LC156" class="blob-code blob-code-inner js-file-line"><span class=pl-c># ==============================================================================</span></td>
      </tr>
      <tr>
        <td id="L157" class="blob-num js-line-number" data-line-number="157"></td>
        <td id="LC157" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L158" class="blob-num js-line-number" data-line-number="158"></td>
        <td id="LC158" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L159" class="blob-num js-line-number" data-line-number="159"></td>
        <td id="LC159" class="blob-code blob-code-inner js-file-line"><span class=pl-k>def</span> <span class=pl-en>get_xy_location_id_dict</span>(<span class=pl-s1>the_scenario</span>, <span class=pl-s1>logger</span>):</td>
      </tr>
      <tr>
        <td id="L160" class="blob-num js-line-number" data-line-number="160"></td>
        <td id="LC160" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start: get_xy_location_id_dict&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L161" class="blob-num js-line-number" data-line-number="161"></td>
        <td id="LC161" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L162" class="blob-num js-line-number" data-line-number="162"></td>
        <td id="LC162" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>with</span> <span class=pl-s1>sqlite3</span>.<span class=pl-en>connect</span>(<span class=pl-s1>the_scenario</span>.<span class=pl-s1>main_db</span>) <span class=pl-k>as</span> <span class=pl-s1>db_con</span>:</td>
      </tr>
      <tr>
        <td id="L163" class="blob-num js-line-number" data-line-number="163"></td>
        <td id="LC163" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L164" class="blob-num js-line-number" data-line-number="164"></td>
        <td id="LC164" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>sql</span> <span class=pl-c1>=</span> <span class=pl-s>&quot;select location_id, shape_x, shape_y from locations;&quot;</span></td>
      </tr>
      <tr>
        <td id="L165" class="blob-num js-line-number" data-line-number="165"></td>
        <td id="LC165" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>db_cur</span> <span class=pl-c1>=</span> <span class=pl-s1>db_con</span>.<span class=pl-en>execute</span>(<span class=pl-s1>sql</span>)</td>
      </tr>
      <tr>
        <td id="L166" class="blob-num js-line-number" data-line-number="166"></td>
        <td id="LC166" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L167" class="blob-num js-line-number" data-line-number="167"></td>
        <td id="LC167" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>xy_location_id_dict</span> <span class=pl-c1>=</span> {}</td>
      </tr>
      <tr>
        <td id="L168" class="blob-num js-line-number" data-line-number="168"></td>
        <td id="LC168" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>for</span> <span class=pl-s1>row</span> <span class=pl-c1>in</span> <span class=pl-s1>db_cur</span>:</td>
      </tr>
      <tr>
        <td id="L169" class="blob-num js-line-number" data-line-number="169"></td>
        <td id="LC169" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>location_id</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>0</span>]</td>
      </tr>
      <tr>
        <td id="L170" class="blob-num js-line-number" data-line-number="170"></td>
        <td id="LC170" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>shape_x</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>1</span>]</td>
      </tr>
      <tr>
        <td id="L171" class="blob-num js-line-number" data-line-number="171"></td>
        <td id="LC171" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>shape_y</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>2</span>]</td>
      </tr>
      <tr>
        <td id="L172" class="blob-num js-line-number" data-line-number="172"></td>
        <td id="LC172" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>xy_location_id_dict</span>[<span class=pl-s1>location_id</span>] <span class=pl-c1>=</span> <span class=pl-s>&quot;[{}, {}]&quot;</span>.<span class=pl-en>format</span>(<span class=pl-s1>shape_x</span>, <span class=pl-s1>shape_y</span>)</td>
      </tr>
      <tr>
        <td id="L173" class="blob-num js-line-number" data-line-number="173"></td>
        <td id="LC173" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L174" class="blob-num js-line-number" data-line-number="174"></td>
        <td id="LC174" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;finish: get_xy_location_id_dict&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L175" class="blob-num js-line-number" data-line-number="175"></td>
        <td id="LC175" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L176" class="blob-num js-line-number" data-line-number="176"></td>
        <td id="LC176" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>return</span> <span class=pl-s1>xy_location_id_dict</span></td>
      </tr>
      <tr>
        <td id="L177" class="blob-num js-line-number" data-line-number="177"></td>
        <td id="LC177" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L178" class="blob-num js-line-number" data-line-number="178"></td>
        <td id="LC178" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L179" class="blob-num js-line-number" data-line-number="179"></td>
        <td id="LC179" class="blob-code blob-code-inner js-file-line"><span class=pl-c># ==============================================================================</span></td>
      </tr>
      <tr>
        <td id="L180" class="blob-num js-line-number" data-line-number="180"></td>
        <td id="LC180" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L181" class="blob-num js-line-number" data-line-number="181"></td>
        <td id="LC181" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L182" class="blob-num js-line-number" data-line-number="182"></td>
        <td id="LC182" class="blob-code blob-code-inner js-file-line"><span class=pl-k>def</span> <span class=pl-en>get_location_id_name_dict</span>(<span class=pl-s1>the_scenario</span>, <span class=pl-s1>logger</span>):</td>
      </tr>
      <tr>
        <td id="L183" class="blob-num js-line-number" data-line-number="183"></td>
        <td id="LC183" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start: get_location_id_name_dict&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L184" class="blob-num js-line-number" data-line-number="184"></td>
        <td id="LC184" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L185" class="blob-num js-line-number" data-line-number="185"></td>
        <td id="LC185" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>location_id_name_dict</span> <span class=pl-c1>=</span> {}</td>
      </tr>
      <tr>
        <td id="L186" class="blob-num js-line-number" data-line-number="186"></td>
        <td id="LC186" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L187" class="blob-num js-line-number" data-line-number="187"></td>
        <td id="LC187" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>with</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>SearchCursor</span>(<span class=pl-s1>the_scenario</span>.<span class=pl-s1>locations_fc</span>, [<span class=pl-s>&quot;location_id_name&quot;</span>, <span class=pl-s>&quot;OBJECTID&quot;</span>]) <span class=pl-k>as</span> <span class=pl-s1>scursor</span>:</td>
      </tr>
      <tr>
        <td id="L188" class="blob-num js-line-number" data-line-number="188"></td>
        <td id="LC188" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L189" class="blob-num js-line-number" data-line-number="189"></td>
        <td id="LC189" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>for</span> <span class=pl-s1>row</span> <span class=pl-c1>in</span> <span class=pl-s1>scursor</span>:</td>
      </tr>
      <tr>
        <td id="L190" class="blob-num js-line-number" data-line-number="190"></td>
        <td id="LC190" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>location_id_name</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>0</span>]</td>
      </tr>
      <tr>
        <td id="L191" class="blob-num js-line-number" data-line-number="191"></td>
        <td id="LC191" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>objectid</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>1</span>]</td>
      </tr>
      <tr>
        <td id="L192" class="blob-num js-line-number" data-line-number="192"></td>
        <td id="LC192" class="blob-code blob-code-inner js-file-line">            <span class=pl-c>#            shape = row[2]</span></td>
      </tr>
      <tr>
        <td id="L193" class="blob-num js-line-number" data-line-number="193"></td>
        <td id="LC193" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>location_id_name_dict</span>[<span class=pl-s1>objectid</span>] <span class=pl-c1>=</span> <span class=pl-s1>location_id_name</span></td>
      </tr>
      <tr>
        <td id="L194" class="blob-num js-line-number" data-line-number="194"></td>
        <td id="LC194" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L195" class="blob-num js-line-number" data-line-number="195"></td>
        <td id="LC195" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;finish: get_location_id_name_dict&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L196" class="blob-num js-line-number" data-line-number="196"></td>
        <td id="LC196" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L197" class="blob-num js-line-number" data-line-number="197"></td>
        <td id="LC197" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>return</span> <span class=pl-s1>location_id_name_dict</span></td>
      </tr>
      <tr>
        <td id="L198" class="blob-num js-line-number" data-line-number="198"></td>
        <td id="LC198" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L199" class="blob-num js-line-number" data-line-number="199"></td>
        <td id="LC199" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L200" class="blob-num js-line-number" data-line-number="200"></td>
        <td id="LC200" class="blob-code blob-code-inner js-file-line"><span class=pl-c># ===============================================================================</span></td>
      </tr>
      <tr>
        <td id="L201" class="blob-num js-line-number" data-line-number="201"></td>
        <td id="LC201" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L202" class="blob-num js-line-number" data-line-number="202"></td>
        <td id="LC202" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L203" class="blob-num js-line-number" data-line-number="203"></td>
        <td id="LC203" class="blob-code blob-code-inner js-file-line"><span class=pl-k>def</span> <span class=pl-en>delete_old_artificial_link</span>(<span class=pl-s1>the_scenario</span>, <span class=pl-s1>logger</span>):</td>
      </tr>
      <tr>
        <td id="L204" class="blob-num js-line-number" data-line-number="204"></td>
        <td id="LC204" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start: delete_old_artificial_link&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L205" class="blob-num js-line-number" data-line-number="205"></td>
        <td id="LC205" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>for</span> <span class=pl-s1>mode</span> <span class=pl-c1>in</span> [<span class=pl-s>&quot;road&quot;</span>, <span class=pl-s>&quot;rail&quot;</span>, <span class=pl-s>&quot;water&quot;</span>, <span class=pl-s>&quot;pipeline_crude_trf_rts&quot;</span>, <span class=pl-s>&quot;pipeline_prod_trf_rts&quot;</span>]:</td>
      </tr>
      <tr>
        <td id="L206" class="blob-num js-line-number" data-line-number="206"></td>
        <td id="LC206" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>edit</span> <span class=pl-c1>=</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>Editor</span>(<span class=pl-s1>the_scenario</span>.<span class=pl-s1>main_gdb</span>)</td>
      </tr>
      <tr>
        <td id="L207" class="blob-num js-line-number" data-line-number="207"></td>
        <td id="LC207" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>edit</span>.<span class=pl-en>startEditing</span>(<span class=pl-c1>False</span>, <span class=pl-c1>False</span>)</td>
      </tr>
      <tr>
        <td id="L208" class="blob-num js-line-number" data-line-number="208"></td>
        <td id="LC208" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>edit</span>.<span class=pl-en>startOperation</span>()</td>
      </tr>
      <tr>
        <td id="L209" class="blob-num js-line-number" data-line-number="209"></td>
        <td id="LC209" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L210" class="blob-num js-line-number" data-line-number="210"></td>
        <td id="LC210" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>with</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>UpdateCursor</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>the_scenario</span>.<span class=pl-s1>main_gdb</span>, <span class=pl-s>&#39;network&#39;</span>, <span class=pl-s1>mode</span>), [<span class=pl-s>&quot;artificial&quot;</span>],</td>
      </tr>
      <tr>
        <td id="L211" class="blob-num js-line-number" data-line-number="211"></td>
        <td id="LC211" class="blob-code blob-code-inner js-file-line">                                   <span class=pl-s1>where_clause</span><span class=pl-c1>=</span><span class=pl-s>&quot;Artificial = 1&quot;</span>) <span class=pl-k>as</span> <span class=pl-s1>cursor</span>:</td>
      </tr>
      <tr>
        <td id="L212" class="blob-num js-line-number" data-line-number="212"></td>
        <td id="LC212" class="blob-code blob-code-inner js-file-line">            <span class=pl-k>for</span> <span class=pl-s1>row</span> <span class=pl-c1>in</span> <span class=pl-s1>cursor</span>:</td>
      </tr>
      <tr>
        <td id="L213" class="blob-num js-line-number" data-line-number="213"></td>
        <td id="LC213" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>cursor</span>.<span class=pl-en>deleteRow</span>()</td>
      </tr>
      <tr>
        <td id="L214" class="blob-num js-line-number" data-line-number="214"></td>
        <td id="LC214" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L215" class="blob-num js-line-number" data-line-number="215"></td>
        <td id="LC215" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>edit</span>.<span class=pl-en>stopOperation</span>()</td>
      </tr>
      <tr>
        <td id="L216" class="blob-num js-line-number" data-line-number="216"></td>
        <td id="LC216" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>edit</span>.<span class=pl-en>stopEditing</span>(<span class=pl-c1>True</span>)</td>
      </tr>
      <tr>
        <td id="L217" class="blob-num js-line-number" data-line-number="217"></td>
        <td id="LC217" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;finish: delete_old_artificial_link&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L218" class="blob-num js-line-number" data-line-number="218"></td>
        <td id="LC218" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L219" class="blob-num js-line-number" data-line-number="219"></td>
        <td id="LC219" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L220" class="blob-num js-line-number" data-line-number="220"></td>
        <td id="LC220" class="blob-code blob-code-inner js-file-line"><span class=pl-c># ===============================================================================</span></td>
      </tr>
      <tr>
        <td id="L221" class="blob-num js-line-number" data-line-number="221"></td>
        <td id="LC221" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L222" class="blob-num js-line-number" data-line-number="222"></td>
        <td id="LC222" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L223" class="blob-num js-line-number" data-line-number="223"></td>
        <td id="LC223" class="blob-code blob-code-inner js-file-line"><span class=pl-k>def</span> <span class=pl-en>cut_lines</span>(<span class=pl-s1>line_list</span>, <span class=pl-s1>point_list</span>, <span class=pl-s1>split_lines</span>):</td>
      </tr>
      <tr>
        <td id="L224" class="blob-num js-line-number" data-line-number="224"></td>
        <td id="LC224" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>for</span> <span class=pl-s1>line</span> <span class=pl-c1>in</span> <span class=pl-s1>line_list</span>:</td>
      </tr>
      <tr>
        <td id="L225" class="blob-num js-line-number" data-line-number="225"></td>
        <td id="LC225" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>is_cut</span> <span class=pl-c1>=</span> <span class=pl-s>&quot;Not Cut&quot;</span></td>
      </tr>
      <tr>
        <td id="L226" class="blob-num js-line-number" data-line-number="226"></td>
        <td id="LC226" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>if</span> <span class=pl-s1>line</span>.<span class=pl-s1>length</span> <span class=pl-c1>&gt;</span> <span class=pl-c1>0.0</span>:  <span class=pl-c># Make sure it&#39;s not an empty geometry.</span></td>
      </tr>
      <tr>
        <td id="L227" class="blob-num js-line-number" data-line-number="227"></td>
        <td id="LC227" class="blob-code blob-code-inner js-file-line">            <span class=pl-k>for</span> <span class=pl-s1>point</span> <span class=pl-c1>in</span> <span class=pl-s1>point_list</span>:</td>
      </tr>
      <tr>
        <td id="L228" class="blob-num js-line-number" data-line-number="228"></td>
        <td id="LC228" class="blob-code blob-code-inner js-file-line">                <span class=pl-c># Even &quot;coincident&quot; points can show up as spatially non-coincident in their</span></td>
      </tr>
      <tr>
        <td id="L229" class="blob-num js-line-number" data-line-number="229"></td>
        <td id="LC229" class="blob-code blob-code-inner js-file-line">                <span class=pl-c># floating-point XY values, so we set up a tolerance.</span></td>
      </tr>
      <tr>
        <td id="L230" class="blob-num js-line-number" data-line-number="230"></td>
        <td id="LC230" class="blob-code blob-code-inner js-file-line">                <span class=pl-k>if</span> <span class=pl-s1>line</span>.<span class=pl-en>distanceTo</span>(<span class=pl-s1>point</span>) <span class=pl-c1>&lt;</span> <span class=pl-c1>1.0</span>:</td>
      </tr>
      <tr>
        <td id="L231" class="blob-num js-line-number" data-line-number="231"></td>
        <td id="LC231" class="blob-code blob-code-inner js-file-line">                    <span class=pl-c># To ensure coincidence, snap the point to the line before proceeding.</span></td>
      </tr>
      <tr>
        <td id="L232" class="blob-num js-line-number" data-line-number="232"></td>
        <td id="LC232" class="blob-code blob-code-inner js-file-line">                    <span class=pl-s1>snap_point</span> <span class=pl-c1>=</span> <span class=pl-s1>line</span>.<span class=pl-en>snapToLine</span>(<span class=pl-s1>point</span>).<span class=pl-s1>firstPoint</span></td>
      </tr>
      <tr>
        <td id="L233" class="blob-num js-line-number" data-line-number="233"></td>
        <td id="LC233" class="blob-code blob-code-inner js-file-line">                    <span class=pl-c># Make sure the point isn&#39;t on a line endpoint, otherwise cutting will produce</span></td>
      </tr>
      <tr>
        <td id="L234" class="blob-num js-line-number" data-line-number="234"></td>
        <td id="LC234" class="blob-code blob-code-inner js-file-line">                    <span class=pl-c># an empty geometry.</span></td>
      </tr>
      <tr>
        <td id="L235" class="blob-num js-line-number" data-line-number="235"></td>
        <td id="LC235" class="blob-code blob-code-inner js-file-line">                    <span class=pl-k>if</span> <span class=pl-c1>not</span> (<span class=pl-s1>snap_point</span>.<span class=pl-en>equals</span>(<span class=pl-s1>line</span>.<span class=pl-s1>lastPoint</span>) <span class=pl-c1>and</span> <span class=pl-s1>snap_point</span>.<span class=pl-en>equals</span>(<span class=pl-s1>line</span>.<span class=pl-s1>firstPoint</span>)):</td>
      </tr>
      <tr>
        <td id="L236" class="blob-num js-line-number" data-line-number="236"></td>
        <td id="LC236" class="blob-code blob-code-inner js-file-line">                        <span class=pl-c># Cut the line. Try it a few different ways to try increase the likelihood it will actually cut</span></td>
      </tr>
      <tr>
        <td id="L237" class="blob-num js-line-number" data-line-number="237"></td>
        <td id="LC237" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s1>cut_line_1</span>, <span class=pl-s1>cut_line_2</span> <span class=pl-c1>=</span> <span class=pl-s1>line</span>.<span class=pl-en>cut</span>(<span class=pl-s1>arcpy</span>.<span class=pl-v>Polyline</span>(<span class=pl-s1>arcpy</span>.<span class=pl-v>Array</span>(</td>
      </tr>
      <tr>
        <td id="L238" class="blob-num js-line-number" data-line-number="238"></td>
        <td id="LC238" class="blob-code blob-code-inner js-file-line">                            [<span class=pl-s1>arcpy</span>.<span class=pl-v>Point</span>(<span class=pl-s1>snap_point</span>.<span class=pl-v>X</span> <span class=pl-c1>+</span> <span class=pl-c1>10.0</span>, <span class=pl-s1>snap_point</span>.<span class=pl-v>Y</span> <span class=pl-c1>+</span> <span class=pl-c1>10.0</span>),</td>
      </tr>
      <tr>
        <td id="L239" class="blob-num js-line-number" data-line-number="239"></td>
        <td id="LC239" class="blob-code blob-code-inner js-file-line">                             <span class=pl-s1>arcpy</span>.<span class=pl-v>Point</span>(<span class=pl-s1>snap_point</span>.<span class=pl-v>X</span> <span class=pl-c1>-</span> <span class=pl-c1>10.0</span>, <span class=pl-s1>snap_point</span>.<span class=pl-v>Y</span> <span class=pl-c1>-</span> <span class=pl-c1>10.0</span>)]), <span class=pl-v>LCC_PROJ</span>))</td>
      </tr>
      <tr>
        <td id="L240" class="blob-num js-line-number" data-line-number="240"></td>
        <td id="LC240" class="blob-code blob-code-inner js-file-line">                        <span class=pl-k>if</span> <span class=pl-s1>cut_line_1</span>.<span class=pl-s1>length</span> <span class=pl-c1>==</span> <span class=pl-c1>0</span> <span class=pl-c1>or</span> <span class=pl-s1>cut_line_2</span>.<span class=pl-s1>length</span> <span class=pl-c1>==</span> <span class=pl-c1>0</span>:</td>
      </tr>
      <tr>
        <td id="L241" class="blob-num js-line-number" data-line-number="241"></td>
        <td id="LC241" class="blob-code blob-code-inner js-file-line">                            <span class=pl-s1>cut_line_1</span>, <span class=pl-s1>cut_line_2</span> <span class=pl-c1>=</span> <span class=pl-s1>line</span>.<span class=pl-en>cut</span>(<span class=pl-s1>arcpy</span>.<span class=pl-v>Polyline</span>(<span class=pl-s1>arcpy</span>.<span class=pl-v>Array</span>(</td>
      </tr>
      <tr>
        <td id="L242" class="blob-num js-line-number" data-line-number="242"></td>
        <td id="LC242" class="blob-code blob-code-inner js-file-line">                                [<span class=pl-s1>arcpy</span>.<span class=pl-v>Point</span>(<span class=pl-s1>snap_point</span>.<span class=pl-v>X</span> <span class=pl-c1>-</span> <span class=pl-c1>10.0</span>, <span class=pl-s1>snap_point</span>.<span class=pl-v>Y</span> <span class=pl-c1>+</span> <span class=pl-c1>10.0</span>),</td>
      </tr>
      <tr>
        <td id="L243" class="blob-num js-line-number" data-line-number="243"></td>
        <td id="LC243" class="blob-code blob-code-inner js-file-line">                                 <span class=pl-s1>arcpy</span>.<span class=pl-v>Point</span>(<span class=pl-s1>snap_point</span>.<span class=pl-v>X</span> <span class=pl-c1>+</span> <span class=pl-c1>10.0</span>, <span class=pl-s1>snap_point</span>.<span class=pl-v>Y</span> <span class=pl-c1>-</span> <span class=pl-c1>10.0</span>)]), <span class=pl-v>LCC_PROJ</span>))</td>
      </tr>
      <tr>
        <td id="L244" class="blob-num js-line-number" data-line-number="244"></td>
        <td id="LC244" class="blob-code blob-code-inner js-file-line">                        <span class=pl-k>if</span> <span class=pl-s1>cut_line_1</span>.<span class=pl-s1>length</span> <span class=pl-c1>==</span> <span class=pl-c1>0</span> <span class=pl-c1>or</span> <span class=pl-s1>cut_line_2</span>.<span class=pl-s1>length</span> <span class=pl-c1>==</span> <span class=pl-c1>0</span>:</td>
      </tr>
      <tr>
        <td id="L245" class="blob-num js-line-number" data-line-number="245"></td>
        <td id="LC245" class="blob-code blob-code-inner js-file-line">                            <span class=pl-s1>cut_line_1</span>, <span class=pl-s1>cut_line_2</span> <span class=pl-c1>=</span> <span class=pl-s1>line</span>.<span class=pl-en>cut</span>(<span class=pl-s1>arcpy</span>.<span class=pl-v>Polyline</span>(<span class=pl-s1>arcpy</span>.<span class=pl-v>Array</span>(</td>
      </tr>
      <tr>
        <td id="L246" class="blob-num js-line-number" data-line-number="246"></td>
        <td id="LC246" class="blob-code blob-code-inner js-file-line">                                [<span class=pl-s1>arcpy</span>.<span class=pl-v>Point</span>(<span class=pl-s1>snap_point</span>.<span class=pl-v>X</span> <span class=pl-c1>+</span> <span class=pl-c1>10.0</span>, <span class=pl-s1>snap_point</span>.<span class=pl-v>Y</span>),</td>
      </tr>
      <tr>
        <td id="L247" class="blob-num js-line-number" data-line-number="247"></td>
        <td id="LC247" class="blob-code blob-code-inner js-file-line">                                 <span class=pl-s1>arcpy</span>.<span class=pl-v>Point</span>(<span class=pl-s1>snap_point</span>.<span class=pl-v>X</span> <span class=pl-c1>-</span> <span class=pl-c1>10.0</span>, <span class=pl-s1>snap_point</span>.<span class=pl-v>Y</span>)]), <span class=pl-v>LCC_PROJ</span>))</td>
      </tr>
      <tr>
        <td id="L248" class="blob-num js-line-number" data-line-number="248"></td>
        <td id="LC248" class="blob-code blob-code-inner js-file-line">                        <span class=pl-k>if</span> <span class=pl-s1>cut_line_1</span>.<span class=pl-s1>length</span> <span class=pl-c1>==</span> <span class=pl-c1>0</span> <span class=pl-c1>or</span> <span class=pl-s1>cut_line_2</span>.<span class=pl-s1>length</span> <span class=pl-c1>==</span> <span class=pl-c1>0</span>:</td>
      </tr>
      <tr>
        <td id="L249" class="blob-num js-line-number" data-line-number="249"></td>
        <td id="LC249" class="blob-code blob-code-inner js-file-line">                            <span class=pl-s1>cut_line_1</span>, <span class=pl-s1>cut_line_2</span> <span class=pl-c1>=</span> <span class=pl-s1>line</span>.<span class=pl-en>cut</span>(<span class=pl-s1>arcpy</span>.<span class=pl-v>Polyline</span>(<span class=pl-s1>arcpy</span>.<span class=pl-v>Array</span>(</td>
      </tr>
      <tr>
        <td id="L250" class="blob-num js-line-number" data-line-number="250"></td>
        <td id="LC250" class="blob-code blob-code-inner js-file-line">                                [<span class=pl-s1>arcpy</span>.<span class=pl-v>Point</span>(<span class=pl-s1>snap_point</span>.<span class=pl-v>X</span>, <span class=pl-s1>snap_point</span>.<span class=pl-v>Y</span> <span class=pl-c1>+</span> <span class=pl-c1>10.0</span>),</td>
      </tr>
      <tr>
        <td id="L251" class="blob-num js-line-number" data-line-number="251"></td>
        <td id="LC251" class="blob-code blob-code-inner js-file-line">                                 <span class=pl-s1>arcpy</span>.<span class=pl-v>Point</span>(<span class=pl-s1>snap_point</span>.<span class=pl-v>X</span>, <span class=pl-s1>snap_point</span>.<span class=pl-v>Y</span> <span class=pl-c1>-</span> <span class=pl-c1>10.0</span>)]), <span class=pl-v>LCC_PROJ</span>))</td>
      </tr>
      <tr>
        <td id="L252" class="blob-num js-line-number" data-line-number="252"></td>
        <td id="LC252" class="blob-code blob-code-inner js-file-line">                        <span class=pl-c># Make sure both descendents have non-zero geometry.</span></td>
      </tr>
      <tr>
        <td id="L253" class="blob-num js-line-number" data-line-number="253"></td>
        <td id="LC253" class="blob-code blob-code-inner js-file-line">                        <span class=pl-k>if</span> <span class=pl-s1>cut_line_1</span>.<span class=pl-s1>length</span> <span class=pl-c1>&gt;</span> <span class=pl-c1>0.0</span> <span class=pl-c1>and</span> <span class=pl-s1>cut_line_2</span>.<span class=pl-s1>length</span> <span class=pl-c1>&gt;</span> <span class=pl-c1>0.0</span>:</td>
      </tr>
      <tr>
        <td id="L254" class="blob-num js-line-number" data-line-number="254"></td>
        <td id="LC254" class="blob-code blob-code-inner js-file-line">                            <span class=pl-c># Feed the cut lines back into the &quot;line&quot; list as candidates to be cut again.</span></td>
      </tr>
      <tr>
        <td id="L255" class="blob-num js-line-number" data-line-number="255"></td>
        <td id="LC255" class="blob-code blob-code-inner js-file-line">                            <span class=pl-s1>line_list</span>.<span class=pl-en>append</span>(<span class=pl-s1>cut_line_1</span>)</td>
      </tr>
      <tr>
        <td id="L256" class="blob-num js-line-number" data-line-number="256"></td>
        <td id="LC256" class="blob-code blob-code-inner js-file-line">                            <span class=pl-s1>line_list</span>.<span class=pl-en>append</span>(<span class=pl-s1>cut_line_2</span>)</td>
      </tr>
      <tr>
        <td id="L257" class="blob-num js-line-number" data-line-number="257"></td>
        <td id="LC257" class="blob-code blob-code-inner js-file-line">                            <span class=pl-s1>line_list</span>.<span class=pl-en>remove</span>(<span class=pl-s1>line</span>)</td>
      </tr>
      <tr>
        <td id="L258" class="blob-num js-line-number" data-line-number="258"></td>
        <td id="LC258" class="blob-code blob-code-inner js-file-line">                            <span class=pl-c># The cut loop will only exit when all lines cannot be cut smaller without producing</span></td>
      </tr>
      <tr>
        <td id="L259" class="blob-num js-line-number" data-line-number="259"></td>
        <td id="LC259" class="blob-code blob-code-inner js-file-line">                            <span class=pl-c># zero-length geometries</span></td>
      </tr>
      <tr>
        <td id="L260" class="blob-num js-line-number" data-line-number="260"></td>
        <td id="LC260" class="blob-code blob-code-inner js-file-line">                            <span class=pl-s1>is_cut</span> <span class=pl-c1>=</span> <span class=pl-s>&quot;Cut&quot;</span></td>
      </tr>
      <tr>
        <td id="L261" class="blob-num js-line-number" data-line-number="261"></td>
        <td id="LC261" class="blob-code blob-code-inner js-file-line">                            <span class=pl-c># break the loop because we&#39;ve cut a line into two now and need to start over.</span></td>
      </tr>
      <tr>
        <td id="L262" class="blob-num js-line-number" data-line-number="262"></td>
        <td id="LC262" class="blob-code blob-code-inner js-file-line">                            <span class=pl-k>break</span></td>
      </tr>
      <tr>
        <td id="L263" class="blob-num js-line-number" data-line-number="263"></td>
        <td id="LC263" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>point_list</span>.<span class=pl-en>remove</span>(<span class=pl-s1>point</span>)</td>
      </tr>
      <tr>
        <td id="L264" class="blob-num js-line-number" data-line-number="264"></td>
        <td id="LC264" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L265" class="blob-num js-line-number" data-line-number="265"></td>
        <td id="LC265" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>if</span> <span class=pl-s1>is_cut</span> <span class=pl-c1>==</span> <span class=pl-s>&quot;Not Cut&quot;</span> <span class=pl-c1>and</span> <span class=pl-en>len</span>(<span class=pl-s1>point_list</span>) <span class=pl-c1>==</span> <span class=pl-c1>0</span>:</td>
      </tr>
      <tr>
        <td id="L266" class="blob-num js-line-number" data-line-number="266"></td>
        <td id="LC266" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>split_lines</span>.<span class=pl-en>append</span>(<span class=pl-s1>line</span>)</td>
      </tr>
      <tr>
        <td id="L267" class="blob-num js-line-number" data-line-number="267"></td>
        <td id="LC267" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>line_list</span>.<span class=pl-en>remove</span>(<span class=pl-s1>line</span>)</td>
      </tr>
      <tr>
        <td id="L268" class="blob-num js-line-number" data-line-number="268"></td>
        <td id="LC268" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L269" class="blob-num js-line-number" data-line-number="269"></td>
        <td id="LC269" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>if</span> <span class=pl-en>len</span>(<span class=pl-s1>line_list</span>) <span class=pl-c1>==</span> <span class=pl-c1>0</span> <span class=pl-c1>and</span> <span class=pl-en>len</span>(<span class=pl-s1>point_list</span>) <span class=pl-c1>==</span> <span class=pl-c1>0</span>:</td>
      </tr>
      <tr>
        <td id="L270" class="blob-num js-line-number" data-line-number="270"></td>
        <td id="LC270" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>continue_iteration</span> <span class=pl-c1>=</span> <span class=pl-s>&#39;done&#39;</span></td>
      </tr>
      <tr>
        <td id="L271" class="blob-num js-line-number" data-line-number="271"></td>
        <td id="LC271" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>else</span>:</td>
      </tr>
      <tr>
        <td id="L272" class="blob-num js-line-number" data-line-number="272"></td>
        <td id="LC272" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>continue_iteration</span> <span class=pl-c1>=</span> <span class=pl-s>&#39;continue running&#39;</span></td>
      </tr>
      <tr>
        <td id="L273" class="blob-num js-line-number" data-line-number="273"></td>
        <td id="LC273" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>return</span> <span class=pl-s1>line_list</span>, <span class=pl-s1>point_list</span>, <span class=pl-s1>split_lines</span>, <span class=pl-s1>continue_iteration</span></td>
      </tr>
      <tr>
        <td id="L274" class="blob-num js-line-number" data-line-number="274"></td>
        <td id="LC274" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L275" class="blob-num js-line-number" data-line-number="275"></td>
        <td id="LC275" class="blob-code blob-code-inner js-file-line"><span class=pl-c># ===============================================================================</span></td>
      </tr>
      <tr>
        <td id="L276" class="blob-num js-line-number" data-line-number="276"></td>
        <td id="LC276" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L277" class="blob-num js-line-number" data-line-number="277"></td>
        <td id="LC277" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L278" class="blob-num js-line-number" data-line-number="278"></td>
        <td id="LC278" class="blob-code blob-code-inner js-file-line"><span class=pl-k>def</span> <span class=pl-en>hook_locations_into_network</span>(<span class=pl-s1>the_scenario</span>, <span class=pl-s1>logger</span>):</td>
      </tr>
      <tr>
        <td id="L279" class="blob-num js-line-number" data-line-number="279"></td>
        <td id="LC279" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L280" class="blob-num js-line-number" data-line-number="280"></td>
        <td id="LC280" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># Add artificial links from the locations feature class into the network</span></td>
      </tr>
      <tr>
        <td id="L281" class="blob-num js-line-number" data-line-number="281"></td>
        <td id="LC281" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># -----------------------------------------------------------------------</span></td>
      </tr>
      <tr>
        <td id="L282" class="blob-num js-line-number" data-line-number="282"></td>
        <td id="LC282" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>info</span>(<span class=pl-s>&quot;start: hook_location_into_network&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L283" class="blob-num js-line-number" data-line-number="283"></td>
        <td id="LC283" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L284" class="blob-num js-line-number" data-line-number="284"></td>
        <td id="LC284" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>scenario_gdb</span> <span class=pl-c1>=</span> <span class=pl-s1>the_scenario</span>.<span class=pl-s1>main_gdb</span></td>
      </tr>
      <tr>
        <td id="L285" class="blob-num js-line-number" data-line-number="285"></td>
        <td id="LC285" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-c1>not</span> <span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>exists</span>(<span class=pl-s1>scenario_gdb</span>):</td>
      </tr>
      <tr>
        <td id="L286" class="blob-num js-line-number" data-line-number="286"></td>
        <td id="LC286" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>error</span> <span class=pl-c1>=</span> <span class=pl-s>&quot;can&#39;t find scenario gdb {}&quot;</span>.<span class=pl-en>format</span>(<span class=pl-s1>scenario_gdb</span>)</td>
      </tr>
      <tr>
        <td id="L287" class="blob-num js-line-number" data-line-number="287"></td>
        <td id="LC287" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>raise</span> <span class=pl-v>IOError</span>(<span class=pl-s1>error</span>)</td>
      </tr>
      <tr>
        <td id="L288" class="blob-num js-line-number" data-line-number="288"></td>
        <td id="LC288" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L289" class="blob-num js-line-number" data-line-number="289"></td>
        <td id="LC289" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># LINKS TO/FROM LOCATIONS</span></td>
      </tr>
      <tr>
        <td id="L290" class="blob-num js-line-number" data-line-number="290"></td>
        <td id="LC290" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># ---------------------------</span></td>
      </tr>
      <tr>
        <td id="L291" class="blob-num js-line-number" data-line-number="291"></td>
        <td id="LC291" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>road_max_artificial_link_distance_miles</span> <span class=pl-c1>=</span> <span class=pl-s1>the_scenario</span>.<span class=pl-s1>road_max_artificial_link_dist</span> <span class=pl-c1>+</span> <span class=pl-s>&quot; Miles&quot;</span></td>
      </tr>
      <tr>
        <td id="L292" class="blob-num js-line-number" data-line-number="292"></td>
        <td id="LC292" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>rail_max_artificial_link_distance_miles</span> <span class=pl-c1>=</span> <span class=pl-s1>the_scenario</span>.<span class=pl-s1>rail_max_artificial_link_dist</span> <span class=pl-c1>+</span> <span class=pl-s>&quot; Miles&quot;</span></td>
      </tr>
      <tr>
        <td id="L293" class="blob-num js-line-number" data-line-number="293"></td>
        <td id="LC293" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>water_max_artificial_link_distance_miles</span> <span class=pl-c1>=</span> <span class=pl-s1>the_scenario</span>.<span class=pl-s1>water_max_artificial_link_dist</span> <span class=pl-c1>+</span> <span class=pl-s>&quot; Miles&quot;</span></td>
      </tr>
      <tr>
        <td id="L294" class="blob-num js-line-number" data-line-number="294"></td>
        <td id="LC294" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>pipeline_crude_max_artificial_link_distance_miles</span> <span class=pl-c1>=</span> <span class=pl-s1>the_scenario</span>.<span class=pl-s1>pipeline_crude_max_artificial_link_dist</span> <span class=pl-c1>+</span> <span class=pl-s>&quot; Miles&quot;</span></td>
      </tr>
      <tr>
        <td id="L295" class="blob-num js-line-number" data-line-number="295"></td>
        <td id="LC295" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>pipeline_prod_max_artificial_link_distance_miles</span> <span class=pl-c1>=</span> <span class=pl-s1>the_scenario</span>.<span class=pl-s1>pipeline_prod_max_artificial_link_dist</span> <span class=pl-c1>+</span> <span class=pl-s>&quot; Miles&quot;</span></td>
      </tr>
      <tr>
        <td id="L296" class="blob-num js-line-number" data-line-number="296"></td>
        <td id="LC296" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L297" class="blob-num js-line-number" data-line-number="297"></td>
        <td id="LC297" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># cleanup any old artificial links</span></td>
      </tr>
      <tr>
        <td id="L298" class="blob-num js-line-number" data-line-number="298"></td>
        <td id="LC298" class="blob-code blob-code-inner js-file-line">    <span class=pl-en>delete_old_artificial_link</span>(<span class=pl-s1>the_scenario</span>, <span class=pl-s1>logger</span>)</td>
      </tr>
      <tr>
        <td id="L299" class="blob-num js-line-number" data-line-number="299"></td>
        <td id="LC299" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L300" class="blob-num js-line-number" data-line-number="300"></td>
        <td id="LC300" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># LINKS TO/FROM LOCATIONS</span></td>
      </tr>
      <tr>
        <td id="L301" class="blob-num js-line-number" data-line-number="301"></td>
        <td id="LC301" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># ---------------------------</span></td>
      </tr>
      <tr>
        <td id="L302" class="blob-num js-line-number" data-line-number="302"></td>
        <td id="LC302" class="blob-code blob-code-inner js-file-line">    <span class=pl-en>locations_add_links</span>(<span class=pl-s1>logger</span>, <span class=pl-s1>the_scenario</span>, <span class=pl-s>&quot;road&quot;</span>, <span class=pl-s1>road_max_artificial_link_distance_miles</span>)</td>
      </tr>
      <tr>
        <td id="L303" class="blob-num js-line-number" data-line-number="303"></td>
        <td id="LC303" class="blob-code blob-code-inner js-file-line">    <span class=pl-en>locations_add_links</span>(<span class=pl-s1>logger</span>, <span class=pl-s1>the_scenario</span>, <span class=pl-s>&quot;rail&quot;</span>, <span class=pl-s1>rail_max_artificial_link_distance_miles</span>)</td>
      </tr>
      <tr>
        <td id="L304" class="blob-num js-line-number" data-line-number="304"></td>
        <td id="LC304" class="blob-code blob-code-inner js-file-line">    <span class=pl-en>locations_add_links</span>(<span class=pl-s1>logger</span>, <span class=pl-s1>the_scenario</span>, <span class=pl-s>&quot;water&quot;</span>, <span class=pl-s1>water_max_artificial_link_distance_miles</span>)</td>
      </tr>
      <tr>
        <td id="L305" class="blob-num js-line-number" data-line-number="305"></td>
        <td id="LC305" class="blob-code blob-code-inner js-file-line">    <span class=pl-en>locations_add_links</span>(<span class=pl-s1>logger</span>, <span class=pl-s1>the_scenario</span>, <span class=pl-s>&quot;pipeline_crude_trf_rts&quot;</span>,</td>
      </tr>
      <tr>
        <td id="L306" class="blob-num js-line-number" data-line-number="306"></td>
        <td id="LC306" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s1>pipeline_crude_max_artificial_link_distance_miles</span>)</td>
      </tr>
      <tr>
        <td id="L307" class="blob-num js-line-number" data-line-number="307"></td>
        <td id="LC307" class="blob-code blob-code-inner js-file-line">    <span class=pl-en>locations_add_links</span>(<span class=pl-s1>logger</span>, <span class=pl-s1>the_scenario</span>, <span class=pl-s>&quot;pipeline_prod_trf_rts&quot;</span>, <span class=pl-s1>pipeline_prod_max_artificial_link_distance_miles</span>)</td>
      </tr>
      <tr>
        <td id="L308" class="blob-num js-line-number" data-line-number="308"></td>
        <td id="LC308" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L309" class="blob-num js-line-number" data-line-number="309"></td>
        <td id="LC309" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># ADD THE SOURCE AND SOURCE_OID FIELDS SO WE CAN MAP THE LINKS IN THE NETWORK TO THE GRAPH EDGES.</span></td>
      </tr>
      <tr>
        <td id="L310" class="blob-num js-line-number" data-line-number="310"></td>
        <td id="LC310" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># -----------------------------------------------------------------------------------------------</span></td>
      </tr>
      <tr>
        <td id="L311" class="blob-num js-line-number" data-line-number="311"></td>
        <td id="LC311" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>for</span> <span class=pl-s1>fc</span> <span class=pl-c1>in</span> [<span class=pl-s>&#39;road&#39;</span>, <span class=pl-s>&#39;rail&#39;</span>, <span class=pl-s>&#39;water&#39;</span>, <span class=pl-s>&#39;pipeline_crude_trf_rts&#39;</span>, <span class=pl-s>&#39;pipeline_prod_trf_rts&#39;</span>, <span class=pl-s>&#39;locations&#39;</span>, <span class=pl-s>&#39;intermodal&#39;</span>,</td>
      </tr>
      <tr>
        <td id="L312" class="blob-num js-line-number" data-line-number="312"></td>
        <td id="LC312" class="blob-code blob-code-inner js-file-line">               <span class=pl-s>&#39;locks&#39;</span>]:</td>
      </tr>
      <tr>
        <td id="L313" class="blob-num js-line-number" data-line-number="313"></td>
        <td id="LC313" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start: processing source and source_OID for: {}&quot;</span>.<span class=pl-en>format</span>(<span class=pl-s1>fc</span>))</td>
      </tr>
      <tr>
        <td id="L314" class="blob-num js-line-number" data-line-number="314"></td>
        <td id="LC314" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>DeleteField_management</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s1>fc</span>), <span class=pl-s>&quot;source&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L315" class="blob-num js-line-number" data-line-number="315"></td>
        <td id="LC315" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>DeleteField_management</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s1>fc</span>), <span class=pl-s>&quot;source_OID&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L316" class="blob-num js-line-number" data-line-number="316"></td>
        <td id="LC316" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s1>fc</span>), <span class=pl-s>&quot;source&quot;</span>, <span class=pl-s>&quot;TEXT&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L317" class="blob-num js-line-number" data-line-number="317"></td>
        <td id="LC317" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s1>fc</span>), <span class=pl-s>&quot;source_OID&quot;</span>, <span class=pl-s>&quot;LONG&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L318" class="blob-num js-line-number" data-line-number="318"></td>
        <td id="LC318" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>CalculateField_management</span>(<span class=pl-s1>in_table</span><span class=pl-c1>=</span><span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s1>fc</span>), <span class=pl-s1>field</span><span class=pl-c1>=</span><span class=pl-s>&quot;source&quot;</span>,</td>
      </tr>
      <tr>
        <td id="L319" class="blob-num js-line-number" data-line-number="319"></td>
        <td id="LC319" class="blob-code blob-code-inner js-file-line">                                        <span class=pl-s1>expression</span><span class=pl-c1>=</span><span class=pl-s>&#39;&quot;{}&quot;&#39;</span>.<span class=pl-en>format</span>(<span class=pl-s1>fc</span>), <span class=pl-s1>expression_type</span><span class=pl-c1>=</span><span class=pl-s>&quot;PYTHON_9.3&quot;</span>, <span class=pl-s1>code_block</span><span class=pl-c1>=</span><span class=pl-s>&quot;&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L320" class="blob-num js-line-number" data-line-number="320"></td>
        <td id="LC320" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>CalculateField_management</span>(<span class=pl-s1>in_table</span><span class=pl-c1>=</span><span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s1>fc</span>), <span class=pl-s1>field</span><span class=pl-c1>=</span><span class=pl-s>&quot;source_OID&quot;</span>,</td>
      </tr>
      <tr>
        <td id="L321" class="blob-num js-line-number" data-line-number="321"></td>
        <td id="LC321" class="blob-code blob-code-inner js-file-line">                                        <span class=pl-s1>expression</span><span class=pl-c1>=</span><span class=pl-s>&quot;!OBJECTID!&quot;</span>, <span class=pl-s1>expression_type</span><span class=pl-c1>=</span><span class=pl-s>&quot;PYTHON_9.3&quot;</span>, <span class=pl-s1>code_block</span><span class=pl-c1>=</span><span class=pl-s>&quot;&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L322" class="blob-num js-line-number" data-line-number="322"></td>
        <td id="LC322" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;finish: processing source_OID for: {}&quot;</span>.<span class=pl-en>format</span>(<span class=pl-s1>fc</span>))</td>
      </tr>
      <tr>
        <td id="L323" class="blob-num js-line-number" data-line-number="323"></td>
        <td id="LC323" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L324" class="blob-num js-line-number" data-line-number="324"></td>
        <td id="LC324" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;finish: hook_location_into_network&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L325" class="blob-num js-line-number" data-line-number="325"></td>
        <td id="LC325" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L326" class="blob-num js-line-number" data-line-number="326"></td>
        <td id="LC326" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L327" class="blob-num js-line-number" data-line-number="327"></td>
        <td id="LC327" class="blob-code blob-code-inner js-file-line"><span class=pl-c># ==============================================================================</span></td>
      </tr>
      <tr>
        <td id="L328" class="blob-num js-line-number" data-line-number="328"></td>
        <td id="LC328" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L329" class="blob-num js-line-number" data-line-number="329"></td>
        <td id="LC329" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L330" class="blob-num js-line-number" data-line-number="330"></td>
        <td id="LC330" class="blob-code blob-code-inner js-file-line"><span class=pl-k>def</span> <span class=pl-en>cache_capacity_information</span>(<span class=pl-s1>the_scenario</span>, <span class=pl-s1>logger</span>):</td>
      </tr>
      <tr>
        <td id="L331" class="blob-num js-line-number" data-line-number="331"></td>
        <td id="LC331" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>info</span>(<span class=pl-s>&quot;start: cache_capacity_information&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L332" class="blob-num js-line-number" data-line-number="332"></td>
        <td id="LC332" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L333" class="blob-num js-line-number" data-line-number="333"></td>
        <td id="LC333" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(</td>
      </tr>
      <tr>
        <td id="L334" class="blob-num js-line-number" data-line-number="334"></td>
        <td id="LC334" class="blob-code blob-code-inner js-file-line">        <span class=pl-s>&quot;export the capacity, volume, and vcr data to the main.db for the locks, pipelines, and intermodal fcs&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L335" class="blob-num js-line-number" data-line-number="335"></td>
        <td id="LC335" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L336" class="blob-num js-line-number" data-line-number="336"></td>
        <td id="LC336" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># need to cache out the pipeline, locks, and intermodal facility capacity information</span></td>
      </tr>
      <tr>
        <td id="L337" class="blob-num js-line-number" data-line-number="337"></td>
        <td id="LC337" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># note source_oid is master_oid for pipeline capacities</span></td>
      </tr>
      <tr>
        <td id="L338" class="blob-num js-line-number" data-line-number="338"></td>
        <td id="LC338" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># capacity_table: source, id_field_name, source_oid, capacity, volume, vcr</span></td>
      </tr>
      <tr>
        <td id="L339" class="blob-num js-line-number" data-line-number="339"></td>
        <td id="LC339" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>with</span> <span class=pl-s1>sqlite3</span>.<span class=pl-en>connect</span>(<span class=pl-s1>the_scenario</span>.<span class=pl-s1>main_db</span>) <span class=pl-k>as</span> <span class=pl-s1>main_db_con</span>:</td>
      </tr>
      <tr>
        <td id="L340" class="blob-num js-line-number" data-line-number="340"></td>
        <td id="LC340" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L341" class="blob-num js-line-number" data-line-number="341"></td>
        <td id="LC341" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># drop the table</span></td>
      </tr>
      <tr>
        <td id="L342" class="blob-num js-line-number" data-line-number="342"></td>
        <td id="LC342" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>sql</span> <span class=pl-c1>=</span> <span class=pl-s>&quot;drop table if exists capacity_nodes&quot;</span></td>
      </tr>
      <tr>
        <td id="L343" class="blob-num js-line-number" data-line-number="343"></td>
        <td id="LC343" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>main_db_con</span>.<span class=pl-en>execute</span>(<span class=pl-s1>sql</span>)</td>
      </tr>
      <tr>
        <td id="L344" class="blob-num js-line-number" data-line-number="344"></td>
        <td id="LC344" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># create the table</span></td>
      </tr>
      <tr>
        <td id="L345" class="blob-num js-line-number" data-line-number="345"></td>
        <td id="LC345" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>sql</span> <span class=pl-c1>=</span> <span class=pl-s>&quot;create table capacity_nodes(&quot;</span> \</td>
      </tr>
      <tr>
        <td id="L346" class="blob-num js-line-number" data-line-number="346"></td>
        <td id="LC346" class="blob-code blob-code-inner js-file-line">              <span class=pl-s>&quot;source text, &quot;</span> \</td>
      </tr>
      <tr>
        <td id="L347" class="blob-num js-line-number" data-line-number="347"></td>
        <td id="LC347" class="blob-code blob-code-inner js-file-line">              <span class=pl-s>&quot;id_field_name text, &quot;</span> \</td>
      </tr>
      <tr>
        <td id="L348" class="blob-num js-line-number" data-line-number="348"></td>
        <td id="LC348" class="blob-code blob-code-inner js-file-line">              <span class=pl-s>&quot;source_OID integer, &quot;</span> \</td>
      </tr>
      <tr>
        <td id="L349" class="blob-num js-line-number" data-line-number="349"></td>
        <td id="LC349" class="blob-code blob-code-inner js-file-line">              <span class=pl-s>&quot;capacity real, &quot;</span> \</td>
      </tr>
      <tr>
        <td id="L350" class="blob-num js-line-number" data-line-number="350"></td>
        <td id="LC350" class="blob-code blob-code-inner js-file-line">              <span class=pl-s>&quot;volume real, &quot;</span> \</td>
      </tr>
      <tr>
        <td id="L351" class="blob-num js-line-number" data-line-number="351"></td>
        <td id="LC351" class="blob-code blob-code-inner js-file-line">              <span class=pl-s>&quot;vcr real&quot;</span> \</td>
      </tr>
      <tr>
        <td id="L352" class="blob-num js-line-number" data-line-number="352"></td>
        <td id="LC352" class="blob-code blob-code-inner js-file-line">              <span class=pl-s>&quot;);&quot;</span></td>
      </tr>
      <tr>
        <td id="L353" class="blob-num js-line-number" data-line-number="353"></td>
        <td id="LC353" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>main_db_con</span>.<span class=pl-en>execute</span>(<span class=pl-s1>sql</span>)</td>
      </tr>
      <tr>
        <td id="L354" class="blob-num js-line-number" data-line-number="354"></td>
        <td id="LC354" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L355" class="blob-num js-line-number" data-line-number="355"></td>
        <td id="LC355" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>for</span> <span class=pl-s1>fc</span> <span class=pl-c1>in</span> [<span class=pl-s>&#39;locks&#39;</span>, <span class=pl-s>&#39;intermodal&#39;</span>, <span class=pl-s>&#39;pipeline_crude&#39;</span>, <span class=pl-s>&#39;pipeline_prod&#39;</span>]:</td>
      </tr>
      <tr>
        <td id="L356" class="blob-num js-line-number" data-line-number="356"></td>
        <td id="LC356" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>capacity_update_list</span> <span class=pl-c1>=</span> []  <span class=pl-c># initialize the list at the beginning of every fc.</span></td>
      </tr>
      <tr>
        <td id="L357" class="blob-num js-line-number" data-line-number="357"></td>
        <td id="LC357" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start: processing fc: {} &quot;</span>.<span class=pl-en>format</span>(<span class=pl-s1>fc</span>))</td>
      </tr>
      <tr>
        <td id="L358" class="blob-num js-line-number" data-line-number="358"></td>
        <td id="LC358" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>if</span> <span class=pl-s>&#39;pipeline&#39;</span> <span class=pl-c1>in</span> <span class=pl-s1>fc</span>:</td>
      </tr>
      <tr>
        <td id="L359" class="blob-num js-line-number" data-line-number="359"></td>
        <td id="LC359" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>id_field_name</span> <span class=pl-c1>=</span> <span class=pl-s>&#39;MASTER_OID&#39;</span></td>
      </tr>
      <tr>
        <td id="L360" class="blob-num js-line-number" data-line-number="360"></td>
        <td id="LC360" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>fields</span> <span class=pl-c1>=</span> [<span class=pl-s>&#39;MASTER_OID&#39;</span>, <span class=pl-s>&#39;Capacity&#39;</span>, <span class=pl-s>&#39;Volume&#39;</span>, <span class=pl-s>&#39;VCR&#39;</span>]</td>
      </tr>
      <tr>
        <td id="L361" class="blob-num js-line-number" data-line-number="361"></td>
        <td id="LC361" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>else</span>:</td>
      </tr>
      <tr>
        <td id="L362" class="blob-num js-line-number" data-line-number="362"></td>
        <td id="LC362" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>id_field_name</span> <span class=pl-c1>=</span> <span class=pl-s>&#39;source_OID&#39;</span></td>
      </tr>
      <tr>
        <td id="L363" class="blob-num js-line-number" data-line-number="363"></td>
        <td id="LC363" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>fields</span> <span class=pl-c1>=</span> [<span class=pl-s>&#39;source_OID&#39;</span>, <span class=pl-s>&#39;Capacity&#39;</span>, <span class=pl-s>&#39;Volume&#39;</span>, <span class=pl-s>&#39;VCR&#39;</span>]</td>
      </tr>
      <tr>
        <td id="L364" class="blob-num js-line-number" data-line-number="364"></td>
        <td id="LC364" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L365" class="blob-num js-line-number" data-line-number="365"></td>
        <td id="LC365" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># do the search cursor:</span></td>
      </tr>
      <tr>
        <td id="L366" class="blob-num js-line-number" data-line-number="366"></td>
        <td id="LC366" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start: search cursor on: fc: {} with fields: {}&quot;</span>.<span class=pl-en>format</span>(<span class=pl-s1>fc</span>, <span class=pl-s1>fields</span>))</td>
      </tr>
      <tr>
        <td id="L367" class="blob-num js-line-number" data-line-number="367"></td>
        <td id="LC367" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>with</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>SearchCursor</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>the_scenario</span>.<span class=pl-s1>main_gdb</span>, <span class=pl-s1>fc</span>), <span class=pl-s1>fields</span>) <span class=pl-k>as</span> <span class=pl-s1>cursor</span>:</td>
      </tr>
      <tr>
        <td id="L368" class="blob-num js-line-number" data-line-number="368"></td>
        <td id="LC368" class="blob-code blob-code-inner js-file-line">            <span class=pl-k>for</span> <span class=pl-s1>row</span> <span class=pl-c1>in</span> <span class=pl-s1>cursor</span>:</td>
      </tr>
      <tr>
        <td id="L369" class="blob-num js-line-number" data-line-number="369"></td>
        <td id="LC369" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>source</span> <span class=pl-c1>=</span> <span class=pl-s1>fc</span></td>
      </tr>
      <tr>
        <td id="L370" class="blob-num js-line-number" data-line-number="370"></td>
        <td id="LC370" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>source_OID</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>0</span>]</td>
      </tr>
      <tr>
        <td id="L371" class="blob-num js-line-number" data-line-number="371"></td>
        <td id="LC371" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>capacity</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>1</span>]</td>
      </tr>
      <tr>
        <td id="L372" class="blob-num js-line-number" data-line-number="372"></td>
        <td id="LC372" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>volume</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>2</span>]</td>
      </tr>
      <tr>
        <td id="L373" class="blob-num js-line-number" data-line-number="373"></td>
        <td id="LC373" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>vcr</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>3</span>]</td>
      </tr>
      <tr>
        <td id="L374" class="blob-num js-line-number" data-line-number="374"></td>
        <td id="LC374" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>capacity_update_list</span>.<span class=pl-en>append</span>([<span class=pl-s1>source</span>, <span class=pl-s1>id_field_name</span>, <span class=pl-s1>source_OID</span>, <span class=pl-s1>capacity</span>, <span class=pl-s1>volume</span>, <span class=pl-s1>vcr</span>])</td>
      </tr>
      <tr>
        <td id="L375" class="blob-num js-line-number" data-line-number="375"></td>
        <td id="LC375" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L376" class="blob-num js-line-number" data-line-number="376"></td>
        <td id="LC376" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start: execute many on: fc: {} with len: {}&quot;</span>.<span class=pl-en>format</span>(<span class=pl-s1>fc</span>, <span class=pl-en>len</span>(<span class=pl-s1>capacity_update_list</span>)))</td>
      </tr>
      <tr>
        <td id="L377" class="blob-num js-line-number" data-line-number="377"></td>
        <td id="LC377" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>if</span> <span class=pl-en>len</span>(<span class=pl-s1>capacity_update_list</span>) <span class=pl-c1>&gt;</span> <span class=pl-c1>0</span>:</td>
      </tr>
      <tr>
        <td id="L378" class="blob-num js-line-number" data-line-number="378"></td>
        <td id="LC378" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>sql</span> <span class=pl-c1>=</span> <span class=pl-s>&quot;insert or ignore into capacity_nodes &quot;</span> \</td>
      </tr>
      <tr>
        <td id="L379" class="blob-num js-line-number" data-line-number="379"></td>
        <td id="LC379" class="blob-code blob-code-inner js-file-line">                  <span class=pl-s>&quot;(source, id_field_name, source_oid, capacity, volume, vcr) &quot;</span> \</td>
      </tr>
      <tr>
        <td id="L380" class="blob-num js-line-number" data-line-number="380"></td>
        <td id="LC380" class="blob-code blob-code-inner js-file-line">                  <span class=pl-s>&quot;values (?, ?, ?, ?, ?, ?);&quot;</span></td>
      </tr>
      <tr>
        <td id="L381" class="blob-num js-line-number" data-line-number="381"></td>
        <td id="LC381" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>main_db_con</span>.<span class=pl-en>executemany</span>(<span class=pl-s1>sql</span>, <span class=pl-s1>capacity_update_list</span>)</td>
      </tr>
      <tr>
        <td id="L382" class="blob-num js-line-number" data-line-number="382"></td>
        <td id="LC382" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>main_db_con</span>.<span class=pl-en>commit</span>()</td>
      </tr>
      <tr>
        <td id="L383" class="blob-num js-line-number" data-line-number="383"></td>
        <td id="LC383" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L384" class="blob-num js-line-number" data-line-number="384"></td>
        <td id="LC384" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># now get the mapping fields from the *_trf_rts and *_trf_sgmnts tables</span></td>
      </tr>
      <tr>
        <td id="L385" class="blob-num js-line-number" data-line-number="385"></td>
        <td id="LC385" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># db table will have the form: source, id_field_name, id, mapping_id_field_name, mapping_id</span></td>
      </tr>
      <tr>
        <td id="L386" class="blob-num js-line-number" data-line-number="386"></td>
        <td id="LC386" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>with</span> <span class=pl-s1>sqlite3</span>.<span class=pl-en>connect</span>(<span class=pl-s1>the_scenario</span>.<span class=pl-s1>main_db</span>) <span class=pl-k>as</span> <span class=pl-s1>main_db_con</span>:</td>
      </tr>
      <tr>
        <td id="L387" class="blob-num js-line-number" data-line-number="387"></td>
        <td id="LC387" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L388" class="blob-num js-line-number" data-line-number="388"></td>
        <td id="LC388" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># drop the table</span></td>
      </tr>
      <tr>
        <td id="L389" class="blob-num js-line-number" data-line-number="389"></td>
        <td id="LC389" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>sql</span> <span class=pl-c1>=</span> <span class=pl-s>&quot;drop table if exists pipeline_mapping&quot;</span></td>
      </tr>
      <tr>
        <td id="L390" class="blob-num js-line-number" data-line-number="390"></td>
        <td id="LC390" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>main_db_con</span>.<span class=pl-en>execute</span>(<span class=pl-s1>sql</span>)</td>
      </tr>
      <tr>
        <td id="L391" class="blob-num js-line-number" data-line-number="391"></td>
        <td id="LC391" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># create the table</span></td>
      </tr>
      <tr>
        <td id="L392" class="blob-num js-line-number" data-line-number="392"></td>
        <td id="LC392" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>sql</span> <span class=pl-c1>=</span> <span class=pl-s>&quot;create table pipeline_mapping(&quot;</span> \</td>
      </tr>
      <tr>
        <td id="L393" class="blob-num js-line-number" data-line-number="393"></td>
        <td id="LC393" class="blob-code blob-code-inner js-file-line">              <span class=pl-s>&quot;source text, &quot;</span> \</td>
      </tr>
      <tr>
        <td id="L394" class="blob-num js-line-number" data-line-number="394"></td>
        <td id="LC394" class="blob-code blob-code-inner js-file-line">              <span class=pl-s>&quot;id_field_name text, &quot;</span> \</td>
      </tr>
      <tr>
        <td id="L395" class="blob-num js-line-number" data-line-number="395"></td>
        <td id="LC395" class="blob-code blob-code-inner js-file-line">              <span class=pl-s>&quot;id integer, &quot;</span> \</td>
      </tr>
      <tr>
        <td id="L396" class="blob-num js-line-number" data-line-number="396"></td>
        <td id="LC396" class="blob-code blob-code-inner js-file-line">              <span class=pl-s>&quot;mapping_id_field_name text, &quot;</span> \</td>
      </tr>
      <tr>
        <td id="L397" class="blob-num js-line-number" data-line-number="397"></td>
        <td id="LC397" class="blob-code blob-code-inner js-file-line">              <span class=pl-s>&quot;mapping_id integer);&quot;</span></td>
      </tr>
      <tr>
        <td id="L398" class="blob-num js-line-number" data-line-number="398"></td>
        <td id="LC398" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>main_db_con</span>.<span class=pl-en>execute</span>(<span class=pl-s1>sql</span>)</td>
      </tr>
      <tr>
        <td id="L399" class="blob-num js-line-number" data-line-number="399"></td>
        <td id="LC399" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L400" class="blob-num js-line-number" data-line-number="400"></td>
        <td id="LC400" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>capacity_update_list</span> <span class=pl-c1>=</span> []</td>
      </tr>
      <tr>
        <td id="L401" class="blob-num js-line-number" data-line-number="401"></td>
        <td id="LC401" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>for</span> <span class=pl-s1>fc</span> <span class=pl-c1>in</span> [<span class=pl-s>&#39;pipeline_crude_trf_rts&#39;</span>, <span class=pl-s>&#39;pipeline_crude_trf_sgmts&#39;</span>, <span class=pl-s>&#39;pipeline_prod_trf_rts&#39;</span>,</td>
      </tr>
      <tr>
        <td id="L402" class="blob-num js-line-number" data-line-number="402"></td>
        <td id="LC402" class="blob-code blob-code-inner js-file-line">               <span class=pl-s>&#39;pipeline_prod_trf_sgmts&#39;</span>]:</td>
      </tr>
      <tr>
        <td id="L403" class="blob-num js-line-number" data-line-number="403"></td>
        <td id="LC403" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L404" class="blob-num js-line-number" data-line-number="404"></td>
        <td id="LC404" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start: processing fc: {} &quot;</span>.<span class=pl-en>format</span>(<span class=pl-s1>fc</span>))</td>
      </tr>
      <tr>
        <td id="L405" class="blob-num js-line-number" data-line-number="405"></td>
        <td id="LC405" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>if</span> <span class=pl-s>&#39;_trf_rts&#39;</span> <span class=pl-c1>in</span> <span class=pl-s1>fc</span>:</td>
      </tr>
      <tr>
        <td id="L406" class="blob-num js-line-number" data-line-number="406"></td>
        <td id="LC406" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>id_field_name</span> <span class=pl-c1>=</span> <span class=pl-s>&#39;source_OID&#39;</span></td>
      </tr>
      <tr>
        <td id="L407" class="blob-num js-line-number" data-line-number="407"></td>
        <td id="LC407" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>mapping_id_field_name</span> <span class=pl-c1>=</span> <span class=pl-s>&#39;tariff_ID&#39;</span></td>
      </tr>
      <tr>
        <td id="L408" class="blob-num js-line-number" data-line-number="408"></td>
        <td id="LC408" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>fields</span> <span class=pl-c1>=</span> [<span class=pl-s>&#39;source_OID&#39;</span>, <span class=pl-s>&#39;tariff_ID&#39;</span>]</td>
      </tr>
      <tr>
        <td id="L409" class="blob-num js-line-number" data-line-number="409"></td>
        <td id="LC409" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>else</span>:</td>
      </tr>
      <tr>
        <td id="L410" class="blob-num js-line-number" data-line-number="410"></td>
        <td id="LC410" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>id_field_name</span> <span class=pl-c1>=</span> <span class=pl-s>&#39;tariff_ID&#39;</span></td>
      </tr>
      <tr>
        <td id="L411" class="blob-num js-line-number" data-line-number="411"></td>
        <td id="LC411" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>mapping_id_field_name</span> <span class=pl-c1>=</span> <span class=pl-s>&#39;MASTER_OID&#39;</span></td>
      </tr>
      <tr>
        <td id="L412" class="blob-num js-line-number" data-line-number="412"></td>
        <td id="LC412" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>fields</span> <span class=pl-c1>=</span> [<span class=pl-s>&#39;tariff_ID&#39;</span>, <span class=pl-s>&#39;MASTER_OID&#39;</span>]</td>
      </tr>
      <tr>
        <td id="L413" class="blob-num js-line-number" data-line-number="413"></td>
        <td id="LC413" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L414" class="blob-num js-line-number" data-line-number="414"></td>
        <td id="LC414" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># do the search cursor:</span></td>
      </tr>
      <tr>
        <td id="L415" class="blob-num js-line-number" data-line-number="415"></td>
        <td id="LC415" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start: search cursor on: fc: {} with fields: {}&quot;</span>.<span class=pl-en>format</span>(<span class=pl-s1>fc</span>, <span class=pl-s1>fields</span>))</td>
      </tr>
      <tr>
        <td id="L416" class="blob-num js-line-number" data-line-number="416"></td>
        <td id="LC416" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>with</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>SearchCursor</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>the_scenario</span>.<span class=pl-s1>main_gdb</span>, <span class=pl-s1>fc</span>), <span class=pl-s1>fields</span>) <span class=pl-k>as</span> <span class=pl-s1>cursor</span>:</td>
      </tr>
      <tr>
        <td id="L417" class="blob-num js-line-number" data-line-number="417"></td>
        <td id="LC417" class="blob-code blob-code-inner js-file-line">            <span class=pl-k>for</span> <span class=pl-s1>row</span> <span class=pl-c1>in</span> <span class=pl-s1>cursor</span>:</td>
      </tr>
      <tr>
        <td id="L418" class="blob-num js-line-number" data-line-number="418"></td>
        <td id="LC418" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L419" class="blob-num js-line-number" data-line-number="419"></td>
        <td id="LC419" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>id</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>0</span>]</td>
      </tr>
      <tr>
        <td id="L420" class="blob-num js-line-number" data-line-number="420"></td>
        <td id="LC420" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>mapping_id</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>1</span>]</td>
      </tr>
      <tr>
        <td id="L421" class="blob-num js-line-number" data-line-number="421"></td>
        <td id="LC421" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>capacity_update_list</span>.<span class=pl-en>append</span>([<span class=pl-s1>fc</span>, <span class=pl-s1>id_field_name</span>, <span class=pl-s1>id</span>, <span class=pl-s1>mapping_id_field_name</span>, <span class=pl-s1>mapping_id</span>])</td>
      </tr>
      <tr>
        <td id="L422" class="blob-num js-line-number" data-line-number="422"></td>
        <td id="LC422" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L423" class="blob-num js-line-number" data-line-number="423"></td>
        <td id="LC423" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start: execute many on: fc: {} with len: {}&quot;</span>.<span class=pl-en>format</span>(<span class=pl-s1>fc</span>, <span class=pl-en>len</span>(<span class=pl-s1>capacity_update_list</span>)))</td>
      </tr>
      <tr>
        <td id="L424" class="blob-num js-line-number" data-line-number="424"></td>
        <td id="LC424" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L425" class="blob-num js-line-number" data-line-number="425"></td>
        <td id="LC425" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-en>len</span>(<span class=pl-s1>capacity_update_list</span>) <span class=pl-c1>&gt;</span> <span class=pl-c1>0</span>:</td>
      </tr>
      <tr>
        <td id="L426" class="blob-num js-line-number" data-line-number="426"></td>
        <td id="LC426" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>sql</span> <span class=pl-c1>=</span> <span class=pl-s>&quot;insert or ignore into pipeline_mapping  &quot;</span> \</td>
      </tr>
      <tr>
        <td id="L427" class="blob-num js-line-number" data-line-number="427"></td>
        <td id="LC427" class="blob-code blob-code-inner js-file-line">              <span class=pl-s>&quot;(source, &quot;</span> \</td>
      </tr>
      <tr>
        <td id="L428" class="blob-num js-line-number" data-line-number="428"></td>
        <td id="LC428" class="blob-code blob-code-inner js-file-line">              <span class=pl-s>&quot;id_field_name, &quot;</span> \</td>
      </tr>
      <tr>
        <td id="L429" class="blob-num js-line-number" data-line-number="429"></td>
        <td id="LC429" class="blob-code blob-code-inner js-file-line">              <span class=pl-s>&quot;id, &quot;</span> \</td>
      </tr>
      <tr>
        <td id="L430" class="blob-num js-line-number" data-line-number="430"></td>
        <td id="LC430" class="blob-code blob-code-inner js-file-line">              <span class=pl-s>&quot;mapping_id_field_name, &quot;</span> \</td>
      </tr>
      <tr>
        <td id="L431" class="blob-num js-line-number" data-line-number="431"></td>
        <td id="LC431" class="blob-code blob-code-inner js-file-line">              <span class=pl-s>&quot;mapping_id) &quot;</span> \</td>
      </tr>
      <tr>
        <td id="L432" class="blob-num js-line-number" data-line-number="432"></td>
        <td id="LC432" class="blob-code blob-code-inner js-file-line">              <span class=pl-s>&quot;values (?, ?, ?, ?, ?);&quot;</span></td>
      </tr>
      <tr>
        <td id="L433" class="blob-num js-line-number" data-line-number="433"></td>
        <td id="LC433" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L434" class="blob-num js-line-number" data-line-number="434"></td>
        <td id="LC434" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>main_db_con</span>.<span class=pl-en>executemany</span>(<span class=pl-s1>sql</span>, <span class=pl-s1>capacity_update_list</span>)</td>
      </tr>
      <tr>
        <td id="L435" class="blob-num js-line-number" data-line-number="435"></td>
        <td id="LC435" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>main_db_con</span>.<span class=pl-en>commit</span>()</td>
      </tr>
      <tr>
        <td id="L436" class="blob-num js-line-number" data-line-number="436"></td>
        <td id="LC436" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L437" class="blob-num js-line-number" data-line-number="437"></td>
        <td id="LC437" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L438" class="blob-num js-line-number" data-line-number="438"></td>
        <td id="LC438" class="blob-code blob-code-inner js-file-line"><span class=pl-c># ==============================================================================</span></td>
      </tr>
      <tr>
        <td id="L439" class="blob-num js-line-number" data-line-number="439"></td>
        <td id="LC439" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L440" class="blob-num js-line-number" data-line-number="440"></td>
        <td id="LC440" class="blob-code blob-code-inner js-file-line"><span class=pl-k>def</span> <span class=pl-en>locations_add_links</span>(<span class=pl-s1>logger</span>, <span class=pl-s1>the_scenario</span>, <span class=pl-s1>modal_layer_name</span>, <span class=pl-s1>max_artificial_link_distance_miles</span>):</td>
      </tr>
      <tr>
        <td id="L441" class="blob-num js-line-number" data-line-number="441"></td>
        <td id="LC441" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L442" class="blob-num js-line-number" data-line-number="442"></td>
        <td id="LC442" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># ADD LINKS LOGIC</span></td>
      </tr>
      <tr>
        <td id="L443" class="blob-num js-line-number" data-line-number="443"></td>
        <td id="LC443" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># first we near the mode to the locations fc</span></td>
      </tr>
      <tr>
        <td id="L444" class="blob-num js-line-number" data-line-number="444"></td>
        <td id="LC444" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># then we iterate through the near table and build up a dictionary of links and all the near XYs on that link.</span></td>
      </tr>
      <tr>
        <td id="L445" class="blob-num js-line-number" data-line-number="445"></td>
        <td id="LC445" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># then we split the links on the mode (except pipeline) and preserve the data of that link.</span></td>
      </tr>
      <tr>
        <td id="L446" class="blob-num js-line-number" data-line-number="446"></td>
        <td id="LC446" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># then we near the locations to the nodes on the now split links.</span></td>
      </tr>
      <tr>
        <td id="L447" class="blob-num js-line-number" data-line-number="447"></td>
        <td id="LC447" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># we ignore locations with near dist == 0 on those nodes.</span></td>
      </tr>
      <tr>
        <td id="L448" class="blob-num js-line-number" data-line-number="448"></td>
        <td id="LC448" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># then we add the artificial link and note which locations got links.</span></td>
      </tr>
      <tr>
        <td id="L449" class="blob-num js-line-number" data-line-number="449"></td>
        <td id="LC449" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># then we set the connects_to  field if the location was connected.</span></td>
      </tr>
      <tr>
        <td id="L450" class="blob-num js-line-number" data-line-number="450"></td>
        <td id="LC450" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L451" class="blob-num js-line-number" data-line-number="451"></td>
        <td id="LC451" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start: locations_add_links for mode: {}&quot;</span>.<span class=pl-en>format</span>(<span class=pl-s1>modal_layer_name</span>))</td>
      </tr>
      <tr>
        <td id="L452" class="blob-num js-line-number" data-line-number="452"></td>
        <td id="LC452" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L453" class="blob-num js-line-number" data-line-number="453"></td>
        <td id="LC453" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>scenario_gdb</span> <span class=pl-c1>=</span> <span class=pl-s1>the_scenario</span>.<span class=pl-s1>main_gdb</span></td>
      </tr>
      <tr>
        <td id="L454" class="blob-num js-line-number" data-line-number="454"></td>
        <td id="LC454" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>fp_to_modal_layer</span> <span class=pl-c1>=</span> <span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;network&quot;</span>, <span class=pl-s1>modal_layer_name</span>)</td>
      </tr>
      <tr>
        <td id="L455" class="blob-num js-line-number" data-line-number="455"></td>
        <td id="LC455" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L456" class="blob-num js-line-number" data-line-number="456"></td>
        <td id="LC456" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>locations_fc</span> <span class=pl-c1>=</span> <span class=pl-s1>the_scenario</span>.<span class=pl-s1>locations_fc</span></td>
      </tr>
      <tr>
        <td id="L457" class="blob-num js-line-number" data-line-number="457"></td>
        <td id="LC457" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>DeleteField_management</span>(<span class=pl-s1>fp_to_modal_layer</span>, <span class=pl-s>&quot;LOCATION_ID&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L458" class="blob-num js-line-number" data-line-number="458"></td>
        <td id="LC458" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s1>modal_layer_name</span>), <span class=pl-s>&quot;LOCATION_ID&quot;</span>, <span class=pl-s>&quot;long&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L459" class="blob-num js-line-number" data-line-number="459"></td>
        <td id="LC459" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L460" class="blob-num js-line-number" data-line-number="460"></td>
        <td id="LC460" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>DeleteField_management</span>(<span class=pl-s1>fp_to_modal_layer</span>, <span class=pl-s>&quot;LOCATION_ID_NAME&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L461" class="blob-num js-line-number" data-line-number="461"></td>
        <td id="LC461" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s1>modal_layer_name</span>), <span class=pl-s>&quot;LOCATION_ID_NAME&quot;</span>, <span class=pl-s>&quot;text&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L462" class="blob-num js-line-number" data-line-number="462"></td>
        <td id="LC462" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L463" class="blob-num js-line-number" data-line-number="463"></td>
        <td id="LC463" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-en>float</span>(<span class=pl-s1>max_artificial_link_distance_miles</span>.<span class=pl-en>strip</span>(<span class=pl-s>&quot; Miles&quot;</span>)) <span class=pl-c1>&lt;</span> <span class=pl-c1>0.0000001</span>:</td>
      </tr>
      <tr>
        <td id="L464" class="blob-num js-line-number" data-line-number="464"></td>
        <td id="LC464" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>logger</span>.<span class=pl-en>warning</span>(<span class=pl-s>&quot;Note: ignoring mode {}. User specified artificial link distance of {}&quot;</span>.<span class=pl-en>format</span>(</td>
      </tr>
      <tr>
        <td id="L465" class="blob-num js-line-number" data-line-number="465"></td>
        <td id="LC465" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>modal_layer_name</span>, <span class=pl-s1>max_artificial_link_distance_miles</span>))</td>
      </tr>
      <tr>
        <td id="L466" class="blob-num js-line-number" data-line-number="466"></td>
        <td id="LC466" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;Setting the definition query to artificial = 99999, so we get an empty dataset for the &quot;</span></td>
      </tr>
      <tr>
        <td id="L467" class="blob-num js-line-number" data-line-number="467"></td>
        <td id="LC467" class="blob-code blob-code-inner js-file-line">                     <span class=pl-s>&quot;make_feature_layer and subsequent near analysis&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L468" class="blob-num js-line-number" data-line-number="468"></td>
        <td id="LC468" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L469" class="blob-num js-line-number" data-line-number="469"></td>
        <td id="LC469" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>definition_query</span> <span class=pl-c1>=</span> <span class=pl-s>&quot;Artificial = 999999&quot;</span>  <span class=pl-c># something to return an empty set</span></td>
      </tr>
      <tr>
        <td id="L470" class="blob-num js-line-number" data-line-number="470"></td>
        <td id="LC470" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>else</span>:</td>
      </tr>
      <tr>
        <td id="L471" class="blob-num js-line-number" data-line-number="471"></td>
        <td id="LC471" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>definition_query</span> <span class=pl-c1>=</span> <span class=pl-s>&quot;Artificial = 0&quot;</span>  <span class=pl-c># the normal def query.</span></td>
      </tr>
      <tr>
        <td id="L472" class="blob-num js-line-number" data-line-number="472"></td>
        <td id="LC472" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L473" class="blob-num js-line-number" data-line-number="473"></td>
        <td id="LC473" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-s>&quot;pipeline&quot;</span> <span class=pl-c1>in</span> <span class=pl-s1>modal_layer_name</span>:</td>
      </tr>
      <tr>
        <td id="L474" class="blob-num js-line-number" data-line-number="474"></td>
        <td id="LC474" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L475" class="blob-num js-line-number" data-line-number="475"></td>
        <td id="LC475" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>if</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>Exists</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;network&quot;</span>, <span class=pl-s1>fp_to_modal_layer</span> <span class=pl-c1>+</span> <span class=pl-s>&quot;_points&quot;</span>)):</td>
      </tr>
      <tr>
        <td id="L476" class="blob-num js-line-number" data-line-number="476"></td>
        <td id="LC476" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;network&quot;</span>, <span class=pl-s1>fp_to_modal_layer</span> <span class=pl-c1>+</span> <span class=pl-s>&quot;_points&quot;</span>))</td>
      </tr>
      <tr>
        <td id="L477" class="blob-num js-line-number" data-line-number="477"></td>
        <td id="LC477" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L478" class="blob-num js-line-number" data-line-number="478"></td>
        <td id="LC478" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># limit near to end points</span></td>
      </tr>
      <tr>
        <td id="L479" class="blob-num js-line-number" data-line-number="479"></td>
        <td id="LC479" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>if</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>CheckProduct</span>(<span class=pl-s>&quot;ArcInfo&quot;</span>) <span class=pl-c1>==</span> <span class=pl-s>&quot;Available&quot;</span>:</td>
      </tr>
      <tr>
        <td id="L480" class="blob-num js-line-number" data-line-number="480"></td>
        <td id="LC480" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>arcpy</span>.<span class=pl-v>FeatureVerticesToPoints_management</span>(<span class=pl-s1>in_features</span><span class=pl-c1>=</span><span class=pl-s1>fp_to_modal_layer</span>,</td>
      </tr>
      <tr>
        <td id="L481" class="blob-num js-line-number" data-line-number="481"></td>
        <td id="LC481" class="blob-code blob-code-inner js-file-line">                                                     <span class=pl-s1>out_feature_class</span><span class=pl-c1>=</span><span class=pl-s1>fp_to_modal_layer</span> <span class=pl-c1>+</span> <span class=pl-s>&quot;_points&quot;</span>,</td>
      </tr>
      <tr>
        <td id="L482" class="blob-num js-line-number" data-line-number="482"></td>
        <td id="LC482" class="blob-code blob-code-inner js-file-line">                                                     <span class=pl-s1>point_location</span><span class=pl-c1>=</span><span class=pl-s>&quot;BOTH_ENDS&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L483" class="blob-num js-line-number" data-line-number="483"></td>
        <td id="LC483" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>else</span>:</td>
      </tr>
      <tr>
        <td id="L484" class="blob-num js-line-number" data-line-number="484"></td>
        <td id="LC484" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>logger</span>.<span class=pl-en>warning</span>(<span class=pl-s>&quot;The Advanced/ArcInfo license level of ArcGIS is not available. Modified feature vertices &quot;</span></td>
      </tr>
      <tr>
        <td id="L485" class="blob-num js-line-number" data-line-number="485"></td>
        <td id="LC485" class="blob-code blob-code-inner js-file-line">                           <span class=pl-s>&quot;process will be run.&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L486" class="blob-num js-line-number" data-line-number="486"></td>
        <td id="LC486" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>arcpy</span>.<span class=pl-v>AddGeometryAttributes_management</span>(<span class=pl-s1>fp_to_modal_layer</span>, <span class=pl-s>&quot;LINE_START_MID_END&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L487" class="blob-num js-line-number" data-line-number="487"></td>
        <td id="LC487" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>arcpy</span>.<span class=pl-v>MakeXYEventLayer_management</span>(<span class=pl-s1>fp_to_modal_layer</span>, <span class=pl-s>&quot;START_X&quot;</span>, <span class=pl-s>&quot;START_Y&quot;</span>,</td>
      </tr>
      <tr>
        <td id="L488" class="blob-num js-line-number" data-line-number="488"></td>
        <td id="LC488" class="blob-code blob-code-inner js-file-line">                                              <span class=pl-s>&quot;modal_start_points_lyr&quot;</span>, <span class=pl-v>LCC_PROJ</span>)</td>
      </tr>
      <tr>
        <td id="L489" class="blob-num js-line-number" data-line-number="489"></td>
        <td id="LC489" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L490" class="blob-num js-line-number" data-line-number="490"></td>
        <td id="LC490" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>arcpy</span>.<span class=pl-v>MakeXYEventLayer_management</span>(<span class=pl-s1>fp_to_modal_layer</span>, <span class=pl-s>&quot;END_X&quot;</span>, <span class=pl-s>&quot;END_Y&quot;</span>,</td>
      </tr>
      <tr>
        <td id="L491" class="blob-num js-line-number" data-line-number="491"></td>
        <td id="LC491" class="blob-code blob-code-inner js-file-line">                                              <span class=pl-s>&quot;modal_end_points_lyr&quot;</span>, <span class=pl-v>LCC_PROJ</span>)</td>
      </tr>
      <tr>
        <td id="L492" class="blob-num js-line-number" data-line-number="492"></td>
        <td id="LC492" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L493" class="blob-num js-line-number" data-line-number="493"></td>
        <td id="LC493" class="blob-code blob-code-inner js-file-line">            <span class=pl-c># Due to tool design, must define the feature class location and name slightly differently (separating</span></td>
      </tr>
      <tr>
        <td id="L494" class="blob-num js-line-number" data-line-number="494"></td>
        <td id="LC494" class="blob-code blob-code-inner js-file-line">            <span class=pl-c># scenario gdb from feature class name. fp_to_modal_layer is identical to scenario_gdb + &quot;network&quot;</span></td>
      </tr>
      <tr>
        <td id="L495" class="blob-num js-line-number" data-line-number="495"></td>
        <td id="LC495" class="blob-code blob-code-inner js-file-line">            <span class=pl-c># + modal_layer_name</span></td>
      </tr>
      <tr>
        <td id="L496" class="blob-num js-line-number" data-line-number="496"></td>
        <td id="LC496" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>arcpy</span>.<span class=pl-v>FeatureClassToFeatureClass_conversion</span>(<span class=pl-s>&quot;modal_start_points_lyr&quot;</span>,</td>
      </tr>
      <tr>
        <td id="L497" class="blob-num js-line-number" data-line-number="497"></td>
        <td id="LC497" class="blob-code blob-code-inner js-file-line">                                                        <span class=pl-s1>scenario_gdb</span>,</td>
      </tr>
      <tr>
        <td id="L498" class="blob-num js-line-number" data-line-number="498"></td>
        <td id="LC498" class="blob-code blob-code-inner js-file-line">                                                        <span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s>&quot;network&quot;</span>, <span class=pl-s1>modal_layer_name</span> <span class=pl-c1>+</span> <span class=pl-s>&quot;_points&quot;</span>))</td>
      </tr>
      <tr>
        <td id="L499" class="blob-num js-line-number" data-line-number="499"></td>
        <td id="LC499" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L500" class="blob-num js-line-number" data-line-number="500"></td>
        <td id="LC500" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>arcpy</span>.<span class=pl-v>Append_management</span>([<span class=pl-s>&quot;modal_end_points_lyr&quot;</span>],</td>
      </tr>
      <tr>
        <td id="L501" class="blob-num js-line-number" data-line-number="501"></td>
        <td id="LC501" class="blob-code blob-code-inner js-file-line">                                    <span class=pl-s1>fp_to_modal_layer</span> <span class=pl-c1>+</span> <span class=pl-s>&quot;_points&quot;</span>, <span class=pl-s>&quot;NO_TEST&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L502" class="blob-num js-line-number" data-line-number="502"></td>
        <td id="LC502" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L503" class="blob-num js-line-number" data-line-number="503"></td>
        <td id="LC503" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s>&quot;modal_start_points_lyr&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L504" class="blob-num js-line-number" data-line-number="504"></td>
        <td id="LC504" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s>&quot;modal_end_points_lyr&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L505" class="blob-num js-line-number" data-line-number="505"></td>
        <td id="LC505" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>arcpy</span>.<span class=pl-v>DeleteField_management</span>(<span class=pl-s1>fp_to_modal_layer</span>, <span class=pl-s>&quot;START_X&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L506" class="blob-num js-line-number" data-line-number="506"></td>
        <td id="LC506" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>arcpy</span>.<span class=pl-v>DeleteField_management</span>(<span class=pl-s1>fp_to_modal_layer</span>, <span class=pl-s>&quot;START_Y&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L507" class="blob-num js-line-number" data-line-number="507"></td>
        <td id="LC507" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>arcpy</span>.<span class=pl-v>DeleteField_management</span>(<span class=pl-s1>fp_to_modal_layer</span>, <span class=pl-s>&quot;MID_X&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L508" class="blob-num js-line-number" data-line-number="508"></td>
        <td id="LC508" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>arcpy</span>.<span class=pl-v>DeleteField_management</span>(<span class=pl-s1>fp_to_modal_layer</span>, <span class=pl-s>&quot;MID_Y&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L509" class="blob-num js-line-number" data-line-number="509"></td>
        <td id="LC509" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>arcpy</span>.<span class=pl-v>DeleteField_management</span>(<span class=pl-s1>fp_to_modal_layer</span>, <span class=pl-s>&quot;END_X&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L510" class="blob-num js-line-number" data-line-number="510"></td>
        <td id="LC510" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>arcpy</span>.<span class=pl-v>DeleteField_management</span>(<span class=pl-s1>fp_to_modal_layer</span>, <span class=pl-s>&quot;END_Y&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L511" class="blob-num js-line-number" data-line-number="511"></td>
        <td id="LC511" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L512" class="blob-num js-line-number" data-line-number="512"></td>
        <td id="LC512" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start:  make_feature_layer_management&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L513" class="blob-num js-line-number" data-line-number="513"></td>
        <td id="LC513" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>MakeFeatureLayer_management</span>(<span class=pl-s1>fp_to_modal_layer</span> <span class=pl-c1>+</span> <span class=pl-s>&quot;_points&quot;</span>, <span class=pl-s>&quot;modal_lyr_&quot;</span> <span class=pl-c1>+</span> <span class=pl-s1>modal_layer_name</span>,</td>
      </tr>
      <tr>
        <td id="L514" class="blob-num js-line-number" data-line-number="514"></td>
        <td id="LC514" class="blob-code blob-code-inner js-file-line">                                          <span class=pl-s1>definition_query</span>)</td>
      </tr>
      <tr>
        <td id="L515" class="blob-num js-line-number" data-line-number="515"></td>
        <td id="LC515" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L516" class="blob-num js-line-number" data-line-number="516"></td>
        <td id="LC516" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>else</span>:</td>
      </tr>
      <tr>
        <td id="L517" class="blob-num js-line-number" data-line-number="517"></td>
        <td id="LC517" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start:  make_feature_layer_management&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L518" class="blob-num js-line-number" data-line-number="518"></td>
        <td id="LC518" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>MakeFeatureLayer_management</span>(<span class=pl-s1>fp_to_modal_layer</span>, <span class=pl-s>&quot;modal_lyr_&quot;</span> <span class=pl-c1>+</span> <span class=pl-s1>modal_layer_name</span>, <span class=pl-s1>definition_query</span>)</td>
      </tr>
      <tr>
        <td id="L519" class="blob-num js-line-number" data-line-number="519"></td>
        <td id="LC519" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L520" class="blob-num js-line-number" data-line-number="520"></td>
        <td id="LC520" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;adding links between locations_fc and mode {} with max dist of {}&quot;</span>.<span class=pl-en>format</span>(<span class=pl-s1>modal_layer_name</span>,</td>
      </tr>
      <tr>
        <td id="L521" class="blob-num js-line-number" data-line-number="521"></td>
        <td id="LC521" class="blob-code blob-code-inner js-file-line">                                                                                            <span class=pl-s1>max_artificial_link_distance_miles</span>))</td>
      </tr>
      <tr>
        <td id="L522" class="blob-num js-line-number" data-line-number="522"></td>
        <td id="LC522" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L523" class="blob-num js-line-number" data-line-number="523"></td>
        <td id="LC523" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>Exists</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_near&quot;</span>)):</td>
      </tr>
      <tr>
        <td id="L524" class="blob-num js-line-number" data-line-number="524"></td>
        <td id="LC524" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start:  delete tmp near&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L525" class="blob-num js-line-number" data-line-number="525"></td>
        <td id="LC525" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_near&quot;</span>))</td>
      </tr>
      <tr>
        <td id="L526" class="blob-num js-line-number" data-line-number="526"></td>
        <td id="LC526" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L527" class="blob-num js-line-number" data-line-number="527"></td>
        <td id="LC527" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start:  generate_near&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L528" class="blob-num js-line-number" data-line-number="528"></td>
        <td id="LC528" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>CheckProduct</span>(<span class=pl-s>&quot;ArcInfo&quot;</span>) <span class=pl-c1>==</span> <span class=pl-s>&quot;Available&quot;</span>:</td>
      </tr>
      <tr>
        <td id="L529" class="blob-num js-line-number" data-line-number="529"></td>
        <td id="LC529" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>GenerateNearTable_analysis</span>(<span class=pl-s1>locations_fc</span>, <span class=pl-s>&quot;modal_lyr_&quot;</span> <span class=pl-c1>+</span> <span class=pl-s1>modal_layer_name</span>,</td>
      </tr>
      <tr>
        <td id="L530" class="blob-num js-line-number" data-line-number="530"></td>
        <td id="LC530" class="blob-code blob-code-inner js-file-line">                                         <span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_near&quot;</span>),</td>
      </tr>
      <tr>
        <td id="L531" class="blob-num js-line-number" data-line-number="531"></td>
        <td id="LC531" class="blob-code blob-code-inner js-file-line">                                         <span class=pl-s1>max_artificial_link_distance_miles</span>, <span class=pl-s>&quot;LOCATION&quot;</span>, <span class=pl-s>&quot;NO_ANGLE&quot;</span>, <span class=pl-s>&quot;CLOSEST&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L532" class="blob-num js-line-number" data-line-number="532"></td>
        <td id="LC532" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L533" class="blob-num js-line-number" data-line-number="533"></td>
        <td id="LC533" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>else</span>:</td>
      </tr>
      <tr>
        <td id="L534" class="blob-num js-line-number" data-line-number="534"></td>
        <td id="LC534" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>logger</span>.<span class=pl-en>warning</span>(<span class=pl-s>&quot;The Advanced/ArcInfo license level of ArcGIS is not available. Modified generate_near process &quot;</span></td>
      </tr>
      <tr>
        <td id="L535" class="blob-num js-line-number" data-line-number="535"></td>
        <td id="LC535" class="blob-code blob-code-inner js-file-line">                       <span class=pl-s>&quot;will be run.&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L536" class="blob-num js-line-number" data-line-number="536"></td>
        <td id="LC536" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># Spatial Join</span></td>
      </tr>
      <tr>
        <td id="L537" class="blob-num js-line-number" data-line-number="537"></td>
        <td id="LC537" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># Workaround for GenerateNearTable not being available for lower-level ArcGIS licenses.</span></td>
      </tr>
      <tr>
        <td id="L538" class="blob-num js-line-number" data-line-number="538"></td>
        <td id="LC538" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L539" class="blob-num js-line-number" data-line-number="539"></td>
        <td id="LC539" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>if</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>Exists</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_spatial_join&quot;</span>)):</td>
      </tr>
      <tr>
        <td id="L540" class="blob-num js-line-number" data-line-number="540"></td>
        <td id="LC540" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_spatial_join&quot;</span>))</td>
      </tr>
      <tr>
        <td id="L541" class="blob-num js-line-number" data-line-number="541"></td>
        <td id="LC541" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L542" class="blob-num js-line-number" data-line-number="542"></td>
        <td id="LC542" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># First, add field to capture joined FID</span></td>
      </tr>
      <tr>
        <td id="L543" class="blob-num js-line-number" data-line-number="543"></td>
        <td id="LC543" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s>&quot;modal_lyr_&quot;</span> <span class=pl-c1>+</span> <span class=pl-s1>modal_layer_name</span>, <span class=pl-s>&quot;Join_FID&quot;</span>, <span class=pl-s>&quot;LONG&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L544" class="blob-num js-line-number" data-line-number="544"></td>
        <td id="LC544" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>CalculateField_management</span>(<span class=pl-s>&quot;modal_lyr_&quot;</span> <span class=pl-c1>+</span> <span class=pl-s1>modal_layer_name</span>, <span class=pl-s>&quot;Join_FID&quot;</span>, <span class=pl-s>&quot;!OBJECTID!&quot;</span>,</td>
      </tr>
      <tr>
        <td id="L545" class="blob-num js-line-number" data-line-number="545"></td>
        <td id="LC545" class="blob-code blob-code-inner js-file-line">                                        <span class=pl-s>&quot;PYTHON_9.3&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L546" class="blob-num js-line-number" data-line-number="546"></td>
        <td id="LC546" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L547" class="blob-num js-line-number" data-line-number="547"></td>
        <td id="LC547" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>SpatialJoin_analysis</span>(<span class=pl-s1>locations_fc</span>, <span class=pl-s>&quot;modal_lyr_&quot;</span> <span class=pl-c1>+</span> <span class=pl-s1>modal_layer_name</span>,</td>
      </tr>
      <tr>
        <td id="L548" class="blob-num js-line-number" data-line-number="548"></td>
        <td id="LC548" class="blob-code blob-code-inner js-file-line">                                   <span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_spatial_join&quot;</span>),</td>
      </tr>
      <tr>
        <td id="L549" class="blob-num js-line-number" data-line-number="549"></td>
        <td id="LC549" class="blob-code blob-code-inner js-file-line">                                   <span class=pl-s1>match_option</span><span class=pl-c1>=</span><span class=pl-s>&quot;CLOSEST&quot;</span>, <span class=pl-s1>search_radius</span><span class=pl-c1>=</span><span class=pl-s1>max_artificial_link_distance_miles</span>)</td>
      </tr>
      <tr>
        <td id="L550" class="blob-num js-line-number" data-line-number="550"></td>
        <td id="LC550" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L551" class="blob-num js-line-number" data-line-number="551"></td>
        <td id="LC551" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>DeleteField_management</span>(<span class=pl-s>&quot;modal_lyr_&quot;</span> <span class=pl-c1>+</span> <span class=pl-s1>modal_layer_name</span>, <span class=pl-s>&quot;Join_FID&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L552" class="blob-num js-line-number" data-line-number="552"></td>
        <td id="LC552" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L553" class="blob-num js-line-number" data-line-number="553"></td>
        <td id="LC553" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># queryPointAndDistance on the original point and corresponding spatial join match</span></td>
      </tr>
      <tr>
        <td id="L554" class="blob-num js-line-number" data-line-number="554"></td>
        <td id="LC554" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># For line in spatial_join:</span></td>
      </tr>
      <tr>
        <td id="L555" class="blob-num js-line-number" data-line-number="555"></td>
        <td id="LC555" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>result_dict</span> <span class=pl-c1>=</span> {}</td>
      </tr>
      <tr>
        <td id="L556" class="blob-num js-line-number" data-line-number="556"></td>
        <td id="LC556" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L557" class="blob-num js-line-number" data-line-number="557"></td>
        <td id="LC557" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>with</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>SearchCursor</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_spatial_join&quot;</span>),</td>
      </tr>
      <tr>
        <td id="L558" class="blob-num js-line-number" data-line-number="558"></td>
        <td id="LC558" class="blob-code blob-code-inner js-file-line">                                   [<span class=pl-s>&quot;Target_FID&quot;</span>, <span class=pl-s>&quot;Join_FID&quot;</span>, <span class=pl-s>&quot;SHAPE@&quot;</span>]) <span class=pl-k>as</span> <span class=pl-s1>scursor1</span>:</td>
      </tr>
      <tr>
        <td id="L559" class="blob-num js-line-number" data-line-number="559"></td>
        <td id="LC559" class="blob-code blob-code-inner js-file-line">            <span class=pl-k>for</span> <span class=pl-s1>row1</span> <span class=pl-c1>in</span> <span class=pl-s1>scursor1</span>:</td>
      </tr>
      <tr>
        <td id="L560" class="blob-num js-line-number" data-line-number="560"></td>
        <td id="LC560" class="blob-code blob-code-inner js-file-line">                <span class=pl-k>with</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>SearchCursor</span>(<span class=pl-s>&quot;modal_lyr_&quot;</span> <span class=pl-c1>+</span> <span class=pl-s1>modal_layer_name</span>,</td>
      </tr>
      <tr>
        <td id="L561" class="blob-num js-line-number" data-line-number="561"></td>
        <td id="LC561" class="blob-code blob-code-inner js-file-line">                                           [<span class=pl-s>&quot;OBJECTID&quot;</span>, <span class=pl-s>&quot;SHAPE@&quot;</span>]) <span class=pl-k>as</span> <span class=pl-s1>scursor2</span>:</td>
      </tr>
      <tr>
        <td id="L562" class="blob-num js-line-number" data-line-number="562"></td>
        <td id="LC562" class="blob-code blob-code-inner js-file-line">                    <span class=pl-k>for</span> <span class=pl-s1>row2</span> <span class=pl-c1>in</span> <span class=pl-s1>scursor2</span>:</td>
      </tr>
      <tr>
        <td id="L563" class="blob-num js-line-number" data-line-number="563"></td>
        <td id="LC563" class="blob-code blob-code-inner js-file-line">                        <span class=pl-k>if</span> <span class=pl-s1>row1</span>[<span class=pl-c1>1</span>] <span class=pl-c1>==</span> <span class=pl-s1>row2</span>[<span class=pl-c1>0</span>]:</td>
      </tr>
      <tr>
        <td id="L564" class="blob-num js-line-number" data-line-number="564"></td>
        <td id="LC564" class="blob-code blob-code-inner js-file-line">                            <span class=pl-k>if</span> <span class=pl-s>&quot;pipeline&quot;</span> <span class=pl-c1>in</span> <span class=pl-s1>modal_layer_name</span>:</td>
      </tr>
      <tr>
        <td id="L565" class="blob-num js-line-number" data-line-number="565"></td>
        <td id="LC565" class="blob-code blob-code-inner js-file-line">                                <span class=pl-s1>result</span> <span class=pl-c1>=</span> <span class=pl-s1>row2</span>[<span class=pl-c1>1</span>].<span class=pl-en>angleAndDistanceTo</span>(<span class=pl-s1>row1</span>[<span class=pl-c1>2</span>], <span class=pl-s>&quot;PLANAR&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L566" class="blob-num js-line-number" data-line-number="566"></td>
        <td id="LC566" class="blob-code blob-code-inner js-file-line">                                <span class=pl-c># Capture the point geometry of the nearest point on the polyline to the location point</span></td>
      </tr>
      <tr>
        <td id="L567" class="blob-num js-line-number" data-line-number="567"></td>
        <td id="LC567" class="blob-code blob-code-inner js-file-line">                                <span class=pl-c># and the minimum distance between the line and the point</span></td>
      </tr>
      <tr>
        <td id="L568" class="blob-num js-line-number" data-line-number="568"></td>
        <td id="LC568" class="blob-code blob-code-inner js-file-line">                                <span class=pl-c># result_dict[in_fid] = [near_fid, from_xy, near_xy, near_dist</span></td>
      </tr>
      <tr>
        <td id="L569" class="blob-num js-line-number" data-line-number="569"></td>
        <td id="LC569" class="blob-code blob-code-inner js-file-line">                                <span class=pl-s1>result_dict</span>[<span class=pl-s1>row1</span>[<span class=pl-c1>0</span>]] <span class=pl-c1>=</span> [<span class=pl-s1>row1</span>[<span class=pl-c1>1</span>], <span class=pl-s1>row1</span>[<span class=pl-c1>2</span>], <span class=pl-s1>row2</span>[<span class=pl-c1>1</span>], <span class=pl-s1>result</span>[<span class=pl-c1>1</span>]]</td>
      </tr>
      <tr>
        <td id="L570" class="blob-num js-line-number" data-line-number="570"></td>
        <td id="LC570" class="blob-code blob-code-inner js-file-line">                            <span class=pl-k>else</span>:</td>
      </tr>
      <tr>
        <td id="L571" class="blob-num js-line-number" data-line-number="571"></td>
        <td id="LC571" class="blob-code blob-code-inner js-file-line">                                <span class=pl-s1>result</span> <span class=pl-c1>=</span> <span class=pl-s1>row2</span>[<span class=pl-c1>1</span>].<span class=pl-en>queryPointAndDistance</span>(<span class=pl-s1>row1</span>[<span class=pl-c1>2</span>], <span class=pl-c1>False</span>)</td>
      </tr>
      <tr>
        <td id="L572" class="blob-num js-line-number" data-line-number="572"></td>
        <td id="LC572" class="blob-code blob-code-inner js-file-line">                                <span class=pl-c># Capture the point geometry of the nearest point on the polyline to the location point</span></td>
      </tr>
      <tr>
        <td id="L573" class="blob-num js-line-number" data-line-number="573"></td>
        <td id="LC573" class="blob-code blob-code-inner js-file-line">                                <span class=pl-c># and the minimum distance between the line and the point</span></td>
      </tr>
      <tr>
        <td id="L574" class="blob-num js-line-number" data-line-number="574"></td>
        <td id="LC574" class="blob-code blob-code-inner js-file-line">                                <span class=pl-c># result_dict[in_fid] = [near_fid, from_xy, near_xy, near_dist</span></td>
      </tr>
      <tr>
        <td id="L575" class="blob-num js-line-number" data-line-number="575"></td>
        <td id="LC575" class="blob-code blob-code-inner js-file-line">                                <span class=pl-s1>result_dict</span>[<span class=pl-s1>row1</span>[<span class=pl-c1>0</span>]] <span class=pl-c1>=</span> [<span class=pl-s1>row1</span>[<span class=pl-c1>1</span>], <span class=pl-s1>row1</span>[<span class=pl-c1>2</span>], <span class=pl-s1>result</span>[<span class=pl-c1>0</span>], <span class=pl-s1>result</span>[<span class=pl-c1>2</span>]]</td>
      </tr>
      <tr>
        <td id="L576" class="blob-num js-line-number" data-line-number="576"></td>
        <td id="LC576" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L577" class="blob-num js-line-number" data-line-number="577"></td>
        <td id="LC577" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># Write to a tmp_near table equivalent to what is create by Generate Near Table tool</span></td>
      </tr>
      <tr>
        <td id="L578" class="blob-num js-line-number" data-line-number="578"></td>
        <td id="LC578" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>CreateTable_management</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_near&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L579" class="blob-num js-line-number" data-line-number="579"></td>
        <td id="LC579" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s>&quot;tmp_near&quot;</span>, <span class=pl-s>&quot;IN_FID&quot;</span>, <span class=pl-s>&quot;LONG&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L580" class="blob-num js-line-number" data-line-number="580"></td>
        <td id="LC580" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s>&quot;tmp_near&quot;</span>, <span class=pl-s>&quot;NEAR_FID&quot;</span>, <span class=pl-s>&quot;LONG&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L581" class="blob-num js-line-number" data-line-number="581"></td>
        <td id="LC581" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s>&quot;tmp_near&quot;</span>, <span class=pl-s>&quot;NEAR_DIST&quot;</span>, <span class=pl-s>&quot;LONG&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L582" class="blob-num js-line-number" data-line-number="582"></td>
        <td id="LC582" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s>&quot;tmp_near&quot;</span>, <span class=pl-s>&quot;FROM_X&quot;</span>, <span class=pl-s>&quot;DOUBLE&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L583" class="blob-num js-line-number" data-line-number="583"></td>
        <td id="LC583" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s>&quot;tmp_near&quot;</span>, <span class=pl-s>&quot;FROM_Y&quot;</span>, <span class=pl-s>&quot;DOUBLE&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L584" class="blob-num js-line-number" data-line-number="584"></td>
        <td id="LC584" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s>&quot;tmp_near&quot;</span>, <span class=pl-s>&quot;NEAR_X&quot;</span>, <span class=pl-s>&quot;DOUBLE&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L585" class="blob-num js-line-number" data-line-number="585"></td>
        <td id="LC585" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s>&quot;tmp_near&quot;</span>, <span class=pl-s>&quot;NEAR_Y&quot;</span>, <span class=pl-s>&quot;DOUBLE&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L586" class="blob-num js-line-number" data-line-number="586"></td>
        <td id="LC586" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L587" class="blob-num js-line-number" data-line-number="587"></td>
        <td id="LC587" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># insert the relevant data into the table</span></td>
      </tr>
      <tr>
        <td id="L588" class="blob-num js-line-number" data-line-number="588"></td>
        <td id="LC588" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>icursor</span> <span class=pl-c1>=</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>InsertCursor</span>(<span class=pl-s>&quot;tmp_near&quot;</span>, [<span class=pl-s>&#39;IN_FID&#39;</span>, <span class=pl-s>&#39;NEAR_FID&#39;</span>, <span class=pl-s>&#39;NEAR_DIST&#39;</span>, <span class=pl-s>&#39;FROM_X&#39;</span>, <span class=pl-s>&#39;FROM_Y&#39;</span>,</td>
      </tr>
      <tr>
        <td id="L589" class="blob-num js-line-number" data-line-number="589"></td>
        <td id="LC589" class="blob-code blob-code-inner js-file-line">                                        <span class=pl-s>&#39;NEAR_X&#39;</span>, <span class=pl-s>&#39;NEAR_Y&#39;</span>])</td>
      </tr>
      <tr>
        <td id="L590" class="blob-num js-line-number" data-line-number="590"></td>
        <td id="LC590" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L591" class="blob-num js-line-number" data-line-number="591"></td>
        <td id="LC591" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>for</span> <span class=pl-s1>in_fid</span> <span class=pl-c1>in</span> <span class=pl-s1>result_dict</span>:</td>
      </tr>
      <tr>
        <td id="L592" class="blob-num js-line-number" data-line-number="592"></td>
        <td id="LC592" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>near_fid</span> <span class=pl-c1>=</span> <span class=pl-s1>result_dict</span>[<span class=pl-s1>in_fid</span>][<span class=pl-c1>0</span>]</td>
      </tr>
      <tr>
        <td id="L593" class="blob-num js-line-number" data-line-number="593"></td>
        <td id="LC593" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>near_distance</span> <span class=pl-c1>=</span> <span class=pl-s1>result_dict</span>[<span class=pl-s1>in_fid</span>][<span class=pl-c1>3</span>]</td>
      </tr>
      <tr>
        <td id="L594" class="blob-num js-line-number" data-line-number="594"></td>
        <td id="LC594" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>from_x</span> <span class=pl-c1>=</span> <span class=pl-s1>result_dict</span>[<span class=pl-s1>in_fid</span>][<span class=pl-c1>1</span>].<span class=pl-s1>firstPoint</span>.<span class=pl-v>X</span></td>
      </tr>
      <tr>
        <td id="L595" class="blob-num js-line-number" data-line-number="595"></td>
        <td id="LC595" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>from_y</span> <span class=pl-c1>=</span> <span class=pl-s1>result_dict</span>[<span class=pl-s1>in_fid</span>][<span class=pl-c1>1</span>].<span class=pl-s1>firstPoint</span>.<span class=pl-v>Y</span></td>
      </tr>
      <tr>
        <td id="L596" class="blob-num js-line-number" data-line-number="596"></td>
        <td id="LC596" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>near_x</span> <span class=pl-c1>=</span> <span class=pl-s1>result_dict</span>[<span class=pl-s1>in_fid</span>][<span class=pl-c1>2</span>].<span class=pl-s1>firstPoint</span>.<span class=pl-v>X</span></td>
      </tr>
      <tr>
        <td id="L597" class="blob-num js-line-number" data-line-number="597"></td>
        <td id="LC597" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>near_y</span> <span class=pl-c1>=</span> <span class=pl-s1>result_dict</span>[<span class=pl-s1>in_fid</span>][<span class=pl-c1>2</span>].<span class=pl-s1>firstPoint</span>.<span class=pl-v>Y</span></td>
      </tr>
      <tr>
        <td id="L598" class="blob-num js-line-number" data-line-number="598"></td>
        <td id="LC598" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>icursor</span>.<span class=pl-en>insertRow</span>([<span class=pl-s1>in_fid</span>, <span class=pl-s1>near_fid</span>, <span class=pl-s1>near_distance</span>, <span class=pl-s1>from_x</span>, <span class=pl-s1>from_y</span>, <span class=pl-s1>near_x</span>, <span class=pl-s1>near_y</span>])</td>
      </tr>
      <tr>
        <td id="L599" class="blob-num js-line-number" data-line-number="599"></td>
        <td id="LC599" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L600" class="blob-num js-line-number" data-line-number="600"></td>
        <td id="LC600" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>del</span> <span class=pl-s1>icursor</span></td>
      </tr>
      <tr>
        <td id="L601" class="blob-num js-line-number" data-line-number="601"></td>
        <td id="LC601" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L602" class="blob-num js-line-number" data-line-number="602"></td>
        <td id="LC602" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_spatial_join&quot;</span>))</td>
      </tr>
      <tr>
        <td id="L603" class="blob-num js-line-number" data-line-number="603"></td>
        <td id="LC603" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L604" class="blob-num js-line-number" data-line-number="604"></td>
        <td id="LC604" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span> <span class=pl-c1>=</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>Editor</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>))</td>
      </tr>
      <tr>
        <td id="L605" class="blob-num js-line-number" data-line-number="605"></td>
        <td id="LC605" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span>.<span class=pl-en>startEditing</span>(<span class=pl-c1>False</span>, <span class=pl-c1>False</span>)</td>
      </tr>
      <tr>
        <td id="L606" class="blob-num js-line-number" data-line-number="606"></td>
        <td id="LC606" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span>.<span class=pl-en>startOperation</span>()</td>
      </tr>
      <tr>
        <td id="L607" class="blob-num js-line-number" data-line-number="607"></td>
        <td id="LC607" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L608" class="blob-num js-line-number" data-line-number="608"></td>
        <td id="LC608" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>id_fieldname</span> <span class=pl-c1>=</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>Describe</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s1>modal_layer_name</span>)).<span class=pl-v>OIDFieldName</span></td>
      </tr>
      <tr>
        <td id="L609" class="blob-num js-line-number" data-line-number="609"></td>
        <td id="LC609" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L610" class="blob-num js-line-number" data-line-number="610"></td>
        <td id="LC610" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>seenids</span> <span class=pl-c1>=</span> {}</td>
      </tr>
      <tr>
        <td id="L611" class="blob-num js-line-number" data-line-number="611"></td>
        <td id="LC611" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L612" class="blob-num js-line-number" data-line-number="612"></td>
        <td id="LC612" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># SPLIT LINKS LOGIC</span></td>
      </tr>
      <tr>
        <td id="L613" class="blob-num js-line-number" data-line-number="613"></td>
        <td id="LC613" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># 1) first search through the tmp_near fc and add points from the near on that link.</span></td>
      </tr>
      <tr>
        <td id="L614" class="blob-num js-line-number" data-line-number="614"></td>
        <td id="LC614" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># 2) next we query the mode layer and get the mode specific data using the near FID.</span></td>
      </tr>
      <tr>
        <td id="L615" class="blob-num js-line-number" data-line-number="615"></td>
        <td id="LC615" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># 3) then we split the old link, and use insert cursor to populate mode specific data into fc for the two new links.</span></td>
      </tr>
      <tr>
        <td id="L616" class="blob-num js-line-number" data-line-number="616"></td>
        <td id="LC616" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># 4) then we delete the old unsplit link</span></td>
      </tr>
      <tr>
        <td id="L617" class="blob-num js-line-number" data-line-number="617"></td>
        <td id="LC617" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start:  split links&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L618" class="blob-num js-line-number" data-line-number="618"></td>
        <td id="LC618" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L619" class="blob-num js-line-number" data-line-number="619"></td>
        <td id="LC619" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>CheckProduct</span>(<span class=pl-s>&quot;ArcInfo&quot;</span>) <span class=pl-c1>!=</span> <span class=pl-s>&quot;Available&quot;</span>:</td>
      </tr>
      <tr>
        <td id="L620" class="blob-num js-line-number" data-line-number="620"></td>
        <td id="LC620" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># Adding warning here rather than within the search cursor loop</span></td>
      </tr>
      <tr>
        <td id="L621" class="blob-num js-line-number" data-line-number="621"></td>
        <td id="LC621" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>logger</span>.<span class=pl-en>warning</span>(</td>
      </tr>
      <tr>
        <td id="L622" class="blob-num js-line-number" data-line-number="622"></td>
        <td id="LC622" class="blob-code blob-code-inner js-file-line">            <span class=pl-s>&quot;The Advanced/ArcInfo license level of ArcGIS is not available. Modified split links process &quot;</span></td>
      </tr>
      <tr>
        <td id="L623" class="blob-num js-line-number" data-line-number="623"></td>
        <td id="LC623" class="blob-code blob-code-inner js-file-line">            <span class=pl-s>&quot;will be run.&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L624" class="blob-num js-line-number" data-line-number="624"></td>
        <td id="LC624" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L625" class="blob-num js-line-number" data-line-number="625"></td>
        <td id="LC625" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>with</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>SearchCursor</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_near&quot;</span>),</td>
      </tr>
      <tr>
        <td id="L626" class="blob-num js-line-number" data-line-number="626"></td>
        <td id="LC626" class="blob-code blob-code-inner js-file-line">                               [<span class=pl-s>&quot;NEAR_FID&quot;</span>, <span class=pl-s>&quot;NEAR_X&quot;</span>, <span class=pl-s>&quot;NEAR_Y&quot;</span>, <span class=pl-s>&quot;NEAR_DIST&quot;</span>]) <span class=pl-k>as</span> <span class=pl-s1>scursor</span>:</td>
      </tr>
      <tr>
        <td id="L627" class="blob-num js-line-number" data-line-number="627"></td>
        <td id="LC627" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L628" class="blob-num js-line-number" data-line-number="628"></td>
        <td id="LC628" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>for</span> <span class=pl-s1>row</span> <span class=pl-c1>in</span> <span class=pl-s1>scursor</span>:</td>
      </tr>
      <tr>
        <td id="L629" class="blob-num js-line-number" data-line-number="629"></td>
        <td id="LC629" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L630" class="blob-num js-line-number" data-line-number="630"></td>
        <td id="LC630" class="blob-code blob-code-inner js-file-line">            <span class=pl-c># if the near distance is 0, then its connected and we don&#39;t need to</span></td>
      </tr>
      <tr>
        <td id="L631" class="blob-num js-line-number" data-line-number="631"></td>
        <td id="LC631" class="blob-code blob-code-inner js-file-line">            <span class=pl-c># split the line.</span></td>
      </tr>
      <tr>
        <td id="L632" class="blob-num js-line-number" data-line-number="632"></td>
        <td id="LC632" class="blob-code blob-code-inner js-file-line">            <span class=pl-k>if</span> <span class=pl-s1>row</span>[<span class=pl-c1>3</span>] <span class=pl-c1>==</span> <span class=pl-c1>0</span>:</td>
      </tr>
      <tr>
        <td id="L633" class="blob-num js-line-number" data-line-number="633"></td>
        <td id="LC633" class="blob-code blob-code-inner js-file-line">                <span class=pl-c># only give debug warnring if not pipeline.</span></td>
      </tr>
      <tr>
        <td id="L634" class="blob-num js-line-number" data-line-number="634"></td>
        <td id="LC634" class="blob-code blob-code-inner js-file-line">                <span class=pl-k>if</span> <span class=pl-s>&quot;pipeline&quot;</span> <span class=pl-c1>not</span> <span class=pl-c1>in</span> <span class=pl-s1>modal_layer_name</span>:</td>
      </tr>
      <tr>
        <td id="L635" class="blob-num js-line-number" data-line-number="635"></td>
        <td id="LC635" class="blob-code blob-code-inner js-file-line">                    <span class=pl-s1>logger</span>.<span class=pl-en>warning</span>(</td>
      </tr>
      <tr>
        <td id="L636" class="blob-num js-line-number" data-line-number="636"></td>
        <td id="LC636" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s>&quot;Split links code: LOCATION MIGHT BE ON THE NETWORK. Ignoring NEAR_FID {} with NEAR_DIST {}&quot;</span>.<span class=pl-en>format</span>(</td>
      </tr>
      <tr>
        <td id="L637" class="blob-num js-line-number" data-line-number="637"></td>
        <td id="LC637" class="blob-code blob-code-inner js-file-line">                            <span class=pl-s1>row</span>[<span class=pl-c1>0</span>], <span class=pl-s1>row</span>[<span class=pl-c1>3</span>]))</td>
      </tr>
      <tr>
        <td id="L638" class="blob-num js-line-number" data-line-number="638"></td>
        <td id="LC638" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L639" class="blob-num js-line-number" data-line-number="639"></td>
        <td id="LC639" class="blob-code blob-code-inner js-file-line">            <span class=pl-k>if</span> <span class=pl-c1>not</span> <span class=pl-s1>row</span>[<span class=pl-c1>3</span>] <span class=pl-c1>==</span> <span class=pl-c1>0</span>:</td>
      </tr>
      <tr>
        <td id="L640" class="blob-num js-line-number" data-line-number="640"></td>
        <td id="LC640" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L641" class="blob-num js-line-number" data-line-number="641"></td>
        <td id="LC641" class="blob-code blob-code-inner js-file-line">                <span class=pl-c># STEP 1: point geoms where to split from the near XY</span></td>
      </tr>
      <tr>
        <td id="L642" class="blob-num js-line-number" data-line-number="642"></td>
        <td id="LC642" class="blob-code blob-code-inner js-file-line">                <span class=pl-c># ---------------------------------------------------</span></td>
      </tr>
      <tr>
        <td id="L643" class="blob-num js-line-number" data-line-number="643"></td>
        <td id="LC643" class="blob-code blob-code-inner js-file-line">                <span class=pl-c># get the line ID to split</span></td>
      </tr>
      <tr>
        <td id="L644" class="blob-num js-line-number" data-line-number="644"></td>
        <td id="LC644" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>theIdToGet</span> <span class=pl-c1>=</span> <span class=pl-en>str</span>(<span class=pl-s1>row</span>[<span class=pl-c1>0</span>])  <span class=pl-c># this is the link id we need.</span></td>
      </tr>
      <tr>
        <td id="L645" class="blob-num js-line-number" data-line-number="645"></td>
        <td id="LC645" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L646" class="blob-num js-line-number" data-line-number="646"></td>
        <td id="LC646" class="blob-code blob-code-inner js-file-line">                <span class=pl-k>if</span> <span class=pl-c1>not</span> <span class=pl-s1>theIdToGet</span> <span class=pl-c1>in</span> <span class=pl-s1>seenids</span>:</td>
      </tr>
      <tr>
        <td id="L647" class="blob-num js-line-number" data-line-number="647"></td>
        <td id="LC647" class="blob-code blob-code-inner js-file-line">                    <span class=pl-s1>seenids</span>[<span class=pl-s1>theIdToGet</span>] <span class=pl-c1>=</span> []</td>
      </tr>
      <tr>
        <td id="L648" class="blob-num js-line-number" data-line-number="648"></td>
        <td id="LC648" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L649" class="blob-num js-line-number" data-line-number="649"></td>
        <td id="LC649" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>point</span> <span class=pl-c1>=</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>Point</span>()</td>
      </tr>
      <tr>
        <td id="L650" class="blob-num js-line-number" data-line-number="650"></td>
        <td id="LC650" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>point</span>.<span class=pl-v>X</span> <span class=pl-c1>=</span> <span class=pl-en>float</span>(<span class=pl-s1>row</span>[<span class=pl-c1>1</span>])</td>
      </tr>
      <tr>
        <td id="L651" class="blob-num js-line-number" data-line-number="651"></td>
        <td id="LC651" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>point</span>.<span class=pl-v>Y</span> <span class=pl-c1>=</span> <span class=pl-en>float</span>(<span class=pl-s1>row</span>[<span class=pl-c1>2</span>])</td>
      </tr>
      <tr>
        <td id="L652" class="blob-num js-line-number" data-line-number="652"></td>
        <td id="LC652" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>point_geom</span> <span class=pl-c1>=</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>PointGeometry</span>(<span class=pl-s1>point</span>, <span class=pl-s1>ftot_supporting_gis</span>.<span class=pl-v>LCC_PROJ</span>)</td>
      </tr>
      <tr>
        <td id="L653" class="blob-num js-line-number" data-line-number="653"></td>
        <td id="LC653" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>seenids</span>[<span class=pl-s1>theIdToGet</span>].<span class=pl-en>append</span>(<span class=pl-s1>point_geom</span>)</td>
      </tr>
      <tr>
        <td id="L654" class="blob-num js-line-number" data-line-number="654"></td>
        <td id="LC654" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L655" class="blob-num js-line-number" data-line-number="655"></td>
        <td id="LC655" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># STEP 2 -- get mode specific data from the link</span></td>
      </tr>
      <tr>
        <td id="L656" class="blob-num js-line-number" data-line-number="656"></td>
        <td id="LC656" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># ------------------------------------------------</span></td>
      </tr>
      <tr>
        <td id="L657" class="blob-num js-line-number" data-line-number="657"></td>
        <td id="LC657" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>if</span> <span class=pl-s>&#39;pipeline&#39;</span> <span class=pl-c1>not</span> <span class=pl-c1>in</span> <span class=pl-s1>modal_layer_name</span>:</td>
      </tr>
      <tr>
        <td id="L658" class="blob-num js-line-number" data-line-number="658"></td>
        <td id="LC658" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L659" class="blob-num js-line-number" data-line-number="659"></td>
        <td id="LC659" class="blob-code blob-code-inner js-file-line">            <span class=pl-k>for</span> <span class=pl-s1>theIdToGet</span> <span class=pl-c1>in</span> <span class=pl-s1>seenids</span>:</td>
      </tr>
      <tr>
        <td id="L660" class="blob-num js-line-number" data-line-number="660"></td>
        <td id="LC660" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L661" class="blob-num js-line-number" data-line-number="661"></td>
        <td id="LC661" class="blob-code blob-code-inner js-file-line">                <span class=pl-c># initialize the variables so we dont get any gremlins.</span></td>
      </tr>
      <tr>
        <td id="L662" class="blob-num js-line-number" data-line-number="662"></td>
        <td id="LC662" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>in_line</span> <span class=pl-c1>=</span> <span class=pl-c1>None</span>  <span class=pl-c># the shape geometry</span></td>
      </tr>
      <tr>
        <td id="L663" class="blob-num js-line-number" data-line-number="663"></td>
        <td id="LC663" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>in_capacity</span> <span class=pl-c1>=</span> <span class=pl-c1>None</span>  <span class=pl-c># road + rail</span></td>
      </tr>
      <tr>
        <td id="L664" class="blob-num js-line-number" data-line-number="664"></td>
        <td id="LC664" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>in_volume</span> <span class=pl-c1>=</span> <span class=pl-c1>None</span>  <span class=pl-c># road + rail</span></td>
      </tr>
      <tr>
        <td id="L665" class="blob-num js-line-number" data-line-number="665"></td>
        <td id="LC665" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>in_vcr</span> <span class=pl-c1>=</span> <span class=pl-c1>None</span>  <span class=pl-c># road + rail | volume to capacity ratio</span></td>
      </tr>
      <tr>
        <td id="L666" class="blob-num js-line-number" data-line-number="666"></td>
        <td id="LC666" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>in_fclass</span> <span class=pl-c1>=</span> <span class=pl-c1>None</span>  <span class=pl-c># road | fclass</span></td>
      </tr>
      <tr>
        <td id="L667" class="blob-num js-line-number" data-line-number="667"></td>
        <td id="LC667" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>in_speed</span> <span class=pl-c1>=</span> <span class=pl-c1>None</span>  <span class=pl-c># road | rounded speed</span></td>
      </tr>
      <tr>
        <td id="L668" class="blob-num js-line-number" data-line-number="668"></td>
        <td id="LC668" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>in_stracnet</span> <span class=pl-c1>=</span> <span class=pl-c1>None</span>  <span class=pl-c># rail</span></td>
      </tr>
      <tr>
        <td id="L669" class="blob-num js-line-number" data-line-number="669"></td>
        <td id="LC669" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>in_density_code</span> <span class=pl-c1>=</span> <span class=pl-c1>None</span>  <span class=pl-c># rail</span></td>
      </tr>
      <tr>
        <td id="L670" class="blob-num js-line-number" data-line-number="670"></td>
        <td id="LC670" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>in_tot_up_dwn</span> <span class=pl-c1>=</span> <span class=pl-c1>None</span>  <span class=pl-c># water</span></td>
      </tr>
      <tr>
        <td id="L671" class="blob-num js-line-number" data-line-number="671"></td>
        <td id="LC671" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L672" class="blob-num js-line-number" data-line-number="672"></td>
        <td id="LC672" class="blob-code blob-code-inner js-file-line">                <span class=pl-k>if</span> <span class=pl-s1>modal_layer_name</span> <span class=pl-c1>==</span> <span class=pl-s>&#39;road&#39;</span>:</td>
      </tr>
      <tr>
        <td id="L673" class="blob-num js-line-number" data-line-number="673"></td>
        <td id="LC673" class="blob-code blob-code-inner js-file-line">                    <span class=pl-k>for</span> <span class=pl-s1>row</span> <span class=pl-c1>in</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>SearchCursor</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s1>modal_layer_name</span>),</td>
      </tr>
      <tr>
        <td id="L674" class="blob-num js-line-number" data-line-number="674"></td>
        <td id="LC674" class="blob-code blob-code-inner js-file-line">                                                     [<span class=pl-s>&quot;SHAPE@&quot;</span>, <span class=pl-s>&quot;Capacity&quot;</span>, <span class=pl-s>&quot;Volume&quot;</span>, <span class=pl-s>&quot;VCR&quot;</span>, <span class=pl-s>&quot;FCLASS&quot;</span>, <span class=pl-s>&quot;ROUNDED_SPEED&quot;</span>],</td>
      </tr>
      <tr>
        <td id="L675" class="blob-num js-line-number" data-line-number="675"></td>
        <td id="LC675" class="blob-code blob-code-inner js-file-line">                                                     <span class=pl-s1>where_clause</span><span class=pl-c1>=</span><span class=pl-s1>id_fieldname</span> <span class=pl-c1>+</span> <span class=pl-s>&quot; = &quot;</span> <span class=pl-c1>+</span> <span class=pl-s1>theIdToGet</span>):</td>
      </tr>
      <tr>
        <td id="L676" class="blob-num js-line-number" data-line-number="676"></td>
        <td id="LC676" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s1>in_line</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>0</span>]</td>
      </tr>
      <tr>
        <td id="L677" class="blob-num js-line-number" data-line-number="677"></td>
        <td id="LC677" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s1>in_capacity</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>1</span>]</td>
      </tr>
      <tr>
        <td id="L678" class="blob-num js-line-number" data-line-number="678"></td>
        <td id="LC678" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s1>in_volume</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>2</span>]</td>
      </tr>
      <tr>
        <td id="L679" class="blob-num js-line-number" data-line-number="679"></td>
        <td id="LC679" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s1>in_vcr</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>3</span>]</td>
      </tr>
      <tr>
        <td id="L680" class="blob-num js-line-number" data-line-number="680"></td>
        <td id="LC680" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s1>in_fclass</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>4</span>]</td>
      </tr>
      <tr>
        <td id="L681" class="blob-num js-line-number" data-line-number="681"></td>
        <td id="LC681" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s1>in_speed</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>5</span>]</td>
      </tr>
      <tr>
        <td id="L682" class="blob-num js-line-number" data-line-number="682"></td>
        <td id="LC682" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L683" class="blob-num js-line-number" data-line-number="683"></td>
        <td id="LC683" class="blob-code blob-code-inner js-file-line">                <span class=pl-k>if</span> <span class=pl-s1>modal_layer_name</span> <span class=pl-c1>==</span> <span class=pl-s>&#39;rail&#39;</span>:</td>
      </tr>
      <tr>
        <td id="L684" class="blob-num js-line-number" data-line-number="684"></td>
        <td id="LC684" class="blob-code blob-code-inner js-file-line">                    <span class=pl-k>for</span> <span class=pl-s1>row</span> <span class=pl-c1>in</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>SearchCursor</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s1>modal_layer_name</span>),</td>
      </tr>
      <tr>
        <td id="L685" class="blob-num js-line-number" data-line-number="685"></td>
        <td id="LC685" class="blob-code blob-code-inner js-file-line">                                                     [<span class=pl-s>&quot;SHAPE@&quot;</span>, <span class=pl-s>&quot;Capacity&quot;</span>, <span class=pl-s>&quot;Volume&quot;</span>, <span class=pl-s>&quot;VCR&quot;</span>, <span class=pl-s>&quot;STRACNET&quot;</span>,</td>
      </tr>
      <tr>
        <td id="L686" class="blob-num js-line-number" data-line-number="686"></td>
        <td id="LC686" class="blob-code blob-code-inner js-file-line">                                                      <span class=pl-s>&quot;DENSITY_CODE&quot;</span>], <span class=pl-s1>where_clause</span><span class=pl-c1>=</span><span class=pl-s1>id_fieldname</span> <span class=pl-c1>+</span> <span class=pl-s>&quot; = &quot;</span> <span class=pl-c1>+</span> <span class=pl-s1>theIdToGet</span>):</td>
      </tr>
      <tr>
        <td id="L687" class="blob-num js-line-number" data-line-number="687"></td>
        <td id="LC687" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s1>in_line</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>0</span>]</td>
      </tr>
      <tr>
        <td id="L688" class="blob-num js-line-number" data-line-number="688"></td>
        <td id="LC688" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s1>in_capacity</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>1</span>]</td>
      </tr>
      <tr>
        <td id="L689" class="blob-num js-line-number" data-line-number="689"></td>
        <td id="LC689" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s1>in_volume</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>2</span>]</td>
      </tr>
      <tr>
        <td id="L690" class="blob-num js-line-number" data-line-number="690"></td>
        <td id="LC690" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s1>in_vcr</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>3</span>]</td>
      </tr>
      <tr>
        <td id="L691" class="blob-num js-line-number" data-line-number="691"></td>
        <td id="LC691" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s1>in_stracnet</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>4</span>]</td>
      </tr>
      <tr>
        <td id="L692" class="blob-num js-line-number" data-line-number="692"></td>
        <td id="LC692" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s1>in_density_code</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>5</span>]</td>
      </tr>
      <tr>
        <td id="L693" class="blob-num js-line-number" data-line-number="693"></td>
        <td id="LC693" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L694" class="blob-num js-line-number" data-line-number="694"></td>
        <td id="LC694" class="blob-code blob-code-inner js-file-line">                <span class=pl-k>if</span> <span class=pl-s1>modal_layer_name</span> <span class=pl-c1>==</span> <span class=pl-s>&#39;water&#39;</span>:</td>
      </tr>
      <tr>
        <td id="L695" class="blob-num js-line-number" data-line-number="695"></td>
        <td id="LC695" class="blob-code blob-code-inner js-file-line">                    <span class=pl-k>for</span> <span class=pl-s1>row</span> <span class=pl-c1>in</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>SearchCursor</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s1>modal_layer_name</span>),</td>
      </tr>
      <tr>
        <td id="L696" class="blob-num js-line-number" data-line-number="696"></td>
        <td id="LC696" class="blob-code blob-code-inner js-file-line">                                                     [<span class=pl-s>&quot;SHAPE@&quot;</span>, <span class=pl-s>&quot;Capacity&quot;</span>, <span class=pl-s>&quot;Volume&quot;</span>, <span class=pl-s>&quot;VCR&quot;</span>, <span class=pl-s>&quot;TOT_UP_DWN&quot;</span>],</td>
      </tr>
      <tr>
        <td id="L697" class="blob-num js-line-number" data-line-number="697"></td>
        <td id="LC697" class="blob-code blob-code-inner js-file-line">                                                     <span class=pl-s1>where_clause</span><span class=pl-c1>=</span><span class=pl-s1>id_fieldname</span> <span class=pl-c1>+</span> <span class=pl-s>&quot; = &quot;</span> <span class=pl-c1>+</span> <span class=pl-s1>theIdToGet</span>):</td>
      </tr>
      <tr>
        <td id="L698" class="blob-num js-line-number" data-line-number="698"></td>
        <td id="LC698" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s1>in_line</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>0</span>]</td>
      </tr>
      <tr>
        <td id="L699" class="blob-num js-line-number" data-line-number="699"></td>
        <td id="LC699" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s1>in_capacity</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>1</span>]</td>
      </tr>
      <tr>
        <td id="L700" class="blob-num js-line-number" data-line-number="700"></td>
        <td id="LC700" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s1>in_volume</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>2</span>]</td>
      </tr>
      <tr>
        <td id="L701" class="blob-num js-line-number" data-line-number="701"></td>
        <td id="LC701" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s1>in_vcr</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>3</span>]</td>
      </tr>
      <tr>
        <td id="L702" class="blob-num js-line-number" data-line-number="702"></td>
        <td id="LC702" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s1>in_tot_up_dwn</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>4</span>]</td>
      </tr>
      <tr>
        <td id="L703" class="blob-num js-line-number" data-line-number="703"></td>
        <td id="LC703" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L704" class="blob-num js-line-number" data-line-number="704"></td>
        <td id="LC704" class="blob-code blob-code-inner js-file-line">                <span class=pl-c># STEP 3: Split and populate with mode specific data from old link</span></td>
      </tr>
      <tr>
        <td id="L705" class="blob-num js-line-number" data-line-number="705"></td>
        <td id="LC705" class="blob-code blob-code-inner js-file-line">                <span class=pl-c># ------------------------------------------------------------------</span></td>
      </tr>
      <tr>
        <td id="L706" class="blob-num js-line-number" data-line-number="706"></td>
        <td id="LC706" class="blob-code blob-code-inner js-file-line">                <span class=pl-k>if</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>CheckProduct</span>(<span class=pl-s>&quot;ArcInfo&quot;</span>) <span class=pl-c1>==</span> <span class=pl-s>&quot;Available&quot;</span>:</td>
      </tr>
      <tr>
        <td id="L707" class="blob-num js-line-number" data-line-number="707"></td>
        <td id="LC707" class="blob-code blob-code-inner js-file-line">                    <span class=pl-s1>split_lines</span> <span class=pl-c1>=</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>management</span>.<span class=pl-v>SplitLineAtPoint</span>(<span class=pl-s1>in_line</span>, <span class=pl-s1>seenids</span>[<span class=pl-s1>theIdToGet</span>], <span class=pl-s1>arcpy</span>.<span class=pl-v>Geometry</span>(), <span class=pl-c1>1</span>)</td>
      </tr>
      <tr>
        <td id="L708" class="blob-num js-line-number" data-line-number="708"></td>
        <td id="LC708" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L709" class="blob-num js-line-number" data-line-number="709"></td>
        <td id="LC709" class="blob-code blob-code-inner js-file-line">                <span class=pl-k>else</span>:</td>
      </tr>
      <tr>
        <td id="L710" class="blob-num js-line-number" data-line-number="710"></td>
        <td id="LC710" class="blob-code blob-code-inner js-file-line">                    <span class=pl-c># This is the alternative approach for those without an Advanced/ArcInfo license</span></td>
      </tr>
      <tr>
        <td id="L711" class="blob-num js-line-number" data-line-number="711"></td>
        <td id="LC711" class="blob-code blob-code-inner js-file-line">                    <span class=pl-s1>point_list</span> <span class=pl-c1>=</span> <span class=pl-s1>seenids</span>[<span class=pl-s1>theIdToGet</span>]</td>
      </tr>
      <tr>
        <td id="L712" class="blob-num js-line-number" data-line-number="712"></td>
        <td id="LC712" class="blob-code blob-code-inner js-file-line">                    <span class=pl-s1>line_list</span> <span class=pl-c1>=</span> [<span class=pl-s1>in_line</span>]</td>
      </tr>
      <tr>
        <td id="L713" class="blob-num js-line-number" data-line-number="713"></td>
        <td id="LC713" class="blob-code blob-code-inner js-file-line">                    <span class=pl-s1>split_lines</span> <span class=pl-c1>=</span> []</td>
      </tr>
      <tr>
        <td id="L714" class="blob-num js-line-number" data-line-number="714"></td>
        <td id="LC714" class="blob-code blob-code-inner js-file-line">                    <span class=pl-s1>continue_iteration</span> <span class=pl-c1>=</span> <span class=pl-s>&#39;continue running&#39;</span></td>
      </tr>
      <tr>
        <td id="L715" class="blob-num js-line-number" data-line-number="715"></td>
        <td id="LC715" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L716" class="blob-num js-line-number" data-line-number="716"></td>
        <td id="LC716" class="blob-code blob-code-inner js-file-line">                    <span class=pl-k>while</span> <span class=pl-s1>continue_iteration</span> <span class=pl-c1>==</span> <span class=pl-s>&#39;continue running&#39;</span>:</td>
      </tr>
      <tr>
        <td id="L717" class="blob-num js-line-number" data-line-number="717"></td>
        <td id="LC717" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s1>line_list</span>, <span class=pl-s1>point_list</span>, <span class=pl-s1>split_lines</span>, <span class=pl-s1>continue_iteration</span> <span class=pl-c1>=</span> <span class=pl-en>cut_lines</span>(<span class=pl-s1>line_list</span>, <span class=pl-s1>point_list</span>, <span class=pl-s1>split_lines</span>)</td>
      </tr>
      <tr>
        <td id="L718" class="blob-num js-line-number" data-line-number="718"></td>
        <td id="LC718" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L719" class="blob-num js-line-number" data-line-number="719"></td>
        <td id="LC719" class="blob-code blob-code-inner js-file-line">                <span class=pl-k>if</span> <span class=pl-c1>not</span> <span class=pl-en>len</span>(<span class=pl-s1>split_lines</span>) <span class=pl-c1>==</span> <span class=pl-c1>1</span>:</td>
      </tr>
      <tr>
        <td id="L720" class="blob-num js-line-number" data-line-number="720"></td>
        <td id="LC720" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L721" class="blob-num js-line-number" data-line-number="721"></td>
        <td id="LC721" class="blob-code blob-code-inner js-file-line">                    <span class=pl-c># ROAD</span></td>
      </tr>
      <tr>
        <td id="L722" class="blob-num js-line-number" data-line-number="722"></td>
        <td id="LC722" class="blob-code blob-code-inner js-file-line">                    <span class=pl-k>if</span> <span class=pl-s1>modal_layer_name</span> <span class=pl-c1>==</span> <span class=pl-s>&#39;road&#39;</span>:</td>
      </tr>
      <tr>
        <td id="L723" class="blob-num js-line-number" data-line-number="723"></td>
        <td id="LC723" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L724" class="blob-num js-line-number" data-line-number="724"></td>
        <td id="LC724" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s1>icursor</span> <span class=pl-c1>=</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>InsertCursor</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s1>modal_layer_name</span>),</td>
      </tr>
      <tr>
        <td id="L725" class="blob-num js-line-number" data-line-number="725"></td>
        <td id="LC725" class="blob-code blob-code-inner js-file-line">                                                        [<span class=pl-s>&#39;SHAPE@&#39;</span>, <span class=pl-s>&#39;Artificial&#39;</span>, <span class=pl-s>&#39;MODE_TYPE&#39;</span>, <span class=pl-s>&#39;MILES&#39;</span>, <span class=pl-s>&#39;FCLASS&#39;</span>,</td>
      </tr>
      <tr>
        <td id="L726" class="blob-num js-line-number" data-line-number="726"></td>
        <td id="LC726" class="blob-code blob-code-inner js-file-line">                                                         <span class=pl-s>&#39;ROUNDED_SPEED&#39;</span>, <span class=pl-s>&#39;Volume&#39;</span>, <span class=pl-s>&#39;Capacity&#39;</span>, <span class=pl-s>&#39;VCR&#39;</span>])</td>
      </tr>
      <tr>
        <td id="L727" class="blob-num js-line-number" data-line-number="727"></td>
        <td id="LC727" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L728" class="blob-num js-line-number" data-line-number="728"></td>
        <td id="LC728" class="blob-code blob-code-inner js-file-line">                        <span class=pl-c># Insert new links that include the mode-specific attributes</span></td>
      </tr>
      <tr>
        <td id="L729" class="blob-num js-line-number" data-line-number="729"></td>
        <td id="LC729" class="blob-code blob-code-inner js-file-line">                        <span class=pl-k>for</span> <span class=pl-s1>new_line</span> <span class=pl-c1>in</span> <span class=pl-s1>split_lines</span>:</td>
      </tr>
      <tr>
        <td id="L730" class="blob-num js-line-number" data-line-number="730"></td>
        <td id="LC730" class="blob-code blob-code-inner js-file-line">                            <span class=pl-s1>len_in_miles</span> <span class=pl-c1>=</span> <span class=pl-v>Q_</span>(<span class=pl-s1>new_line</span>.<span class=pl-s1>length</span>, <span class=pl-s>&quot;meters&quot;</span>).<span class=pl-en>to</span>(<span class=pl-s>&quot;miles&quot;</span>).<span class=pl-s1>magnitude</span></td>
      </tr>
      <tr>
        <td id="L731" class="blob-num js-line-number" data-line-number="731"></td>
        <td id="LC731" class="blob-code blob-code-inner js-file-line">                            <span class=pl-s1>icursor</span>.<span class=pl-en>insertRow</span>(</td>
      </tr>
      <tr>
        <td id="L732" class="blob-num js-line-number" data-line-number="732"></td>
        <td id="LC732" class="blob-code blob-code-inner js-file-line">                                [<span class=pl-s1>new_line</span>, <span class=pl-c1>0</span>, <span class=pl-s1>modal_layer_name</span>, <span class=pl-s1>len_in_miles</span>, <span class=pl-s1>in_fclass</span>, <span class=pl-s1>in_speed</span>, <span class=pl-s1>in_volume</span>,</td>
      </tr>
      <tr>
        <td id="L733" class="blob-num js-line-number" data-line-number="733"></td>
        <td id="LC733" class="blob-code blob-code-inner js-file-line">                                 <span class=pl-s1>in_capacity</span>, <span class=pl-s1>in_vcr</span>])</td>
      </tr>
      <tr>
        <td id="L734" class="blob-num js-line-number" data-line-number="734"></td>
        <td id="LC734" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L735" class="blob-num js-line-number" data-line-number="735"></td>
        <td id="LC735" class="blob-code blob-code-inner js-file-line">                        <span class=pl-c># Delete cursor object</span></td>
      </tr>
      <tr>
        <td id="L736" class="blob-num js-line-number" data-line-number="736"></td>
        <td id="LC736" class="blob-code blob-code-inner js-file-line">                        <span class=pl-k>del</span> <span class=pl-s1>icursor</span></td>
      </tr>
      <tr>
        <td id="L737" class="blob-num js-line-number" data-line-number="737"></td>
        <td id="LC737" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L738" class="blob-num js-line-number" data-line-number="738"></td>
        <td id="LC738" class="blob-code blob-code-inner js-file-line">                    <span class=pl-k>elif</span> <span class=pl-s1>modal_layer_name</span> <span class=pl-c1>==</span> <span class=pl-s>&#39;rail&#39;</span>:</td>
      </tr>
      <tr>
        <td id="L739" class="blob-num js-line-number" data-line-number="739"></td>
        <td id="LC739" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s1>icursor</span> <span class=pl-c1>=</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>InsertCursor</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s1>modal_layer_name</span>),</td>
      </tr>
      <tr>
        <td id="L740" class="blob-num js-line-number" data-line-number="740"></td>
        <td id="LC740" class="blob-code blob-code-inner js-file-line">                                                        [<span class=pl-s>&#39;SHAPE@&#39;</span>, <span class=pl-s>&#39;Artificial&#39;</span>, <span class=pl-s>&#39;MODE_TYPE&#39;</span>, <span class=pl-s>&#39;MILES&#39;</span>, <span class=pl-s>&#39;STRACNET&#39;</span>,</td>
      </tr>
      <tr>
        <td id="L741" class="blob-num js-line-number" data-line-number="741"></td>
        <td id="LC741" class="blob-code blob-code-inner js-file-line">                                                         <span class=pl-s>&#39;DENSITY_CODE&#39;</span>, <span class=pl-s>&#39;Volume&#39;</span>, <span class=pl-s>&#39;Capacity&#39;</span>, <span class=pl-s>&#39;VCR&#39;</span>])</td>
      </tr>
      <tr>
        <td id="L742" class="blob-num js-line-number" data-line-number="742"></td>
        <td id="LC742" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L743" class="blob-num js-line-number" data-line-number="743"></td>
        <td id="LC743" class="blob-code blob-code-inner js-file-line">                        <span class=pl-c># Insert new rows that include the mode-specific attributes</span></td>
      </tr>
      <tr>
        <td id="L744" class="blob-num js-line-number" data-line-number="744"></td>
        <td id="LC744" class="blob-code blob-code-inner js-file-line">                        <span class=pl-k>for</span> <span class=pl-s1>new_line</span> <span class=pl-c1>in</span> <span class=pl-s1>split_lines</span>:</td>
      </tr>
      <tr>
        <td id="L745" class="blob-num js-line-number" data-line-number="745"></td>
        <td id="LC745" class="blob-code blob-code-inner js-file-line">                            <span class=pl-s1>len_in_miles</span> <span class=pl-c1>=</span> <span class=pl-v>Q_</span>(<span class=pl-s1>new_line</span>.<span class=pl-s1>length</span>, <span class=pl-s>&quot;meters&quot;</span>).<span class=pl-en>to</span>(<span class=pl-s>&quot;miles&quot;</span>).<span class=pl-s1>magnitude</span></td>
      </tr>
      <tr>
        <td id="L746" class="blob-num js-line-number" data-line-number="746"></td>
        <td id="LC746" class="blob-code blob-code-inner js-file-line">                            <span class=pl-s1>icursor</span>.<span class=pl-en>insertRow</span>(</td>
      </tr>
      <tr>
        <td id="L747" class="blob-num js-line-number" data-line-number="747"></td>
        <td id="LC747" class="blob-code blob-code-inner js-file-line">                                [<span class=pl-s1>new_line</span>, <span class=pl-c1>0</span>, <span class=pl-s1>modal_layer_name</span>, <span class=pl-s1>len_in_miles</span>, <span class=pl-s1>in_stracnet</span>, <span class=pl-s1>in_density_code</span>, <span class=pl-s1>in_volume</span>,</td>
      </tr>
      <tr>
        <td id="L748" class="blob-num js-line-number" data-line-number="748"></td>
        <td id="LC748" class="blob-code blob-code-inner js-file-line">                                 <span class=pl-s1>in_capacity</span>, <span class=pl-s1>in_vcr</span>])</td>
      </tr>
      <tr>
        <td id="L749" class="blob-num js-line-number" data-line-number="749"></td>
        <td id="LC749" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L750" class="blob-num js-line-number" data-line-number="750"></td>
        <td id="LC750" class="blob-code blob-code-inner js-file-line">                        <span class=pl-c># Delete cursor object</span></td>
      </tr>
      <tr>
        <td id="L751" class="blob-num js-line-number" data-line-number="751"></td>
        <td id="LC751" class="blob-code blob-code-inner js-file-line">                        <span class=pl-k>del</span> <span class=pl-s1>icursor</span></td>
      </tr>
      <tr>
        <td id="L752" class="blob-num js-line-number" data-line-number="752"></td>
        <td id="LC752" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L753" class="blob-num js-line-number" data-line-number="753"></td>
        <td id="LC753" class="blob-code blob-code-inner js-file-line">                    <span class=pl-k>elif</span> <span class=pl-s1>modal_layer_name</span> <span class=pl-c1>==</span> <span class=pl-s>&#39;water&#39;</span>:</td>
      </tr>
      <tr>
        <td id="L754" class="blob-num js-line-number" data-line-number="754"></td>
        <td id="LC754" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L755" class="blob-num js-line-number" data-line-number="755"></td>
        <td id="LC755" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s1>icursor</span> <span class=pl-c1>=</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>InsertCursor</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s1>modal_layer_name</span>),</td>
      </tr>
      <tr>
        <td id="L756" class="blob-num js-line-number" data-line-number="756"></td>
        <td id="LC756" class="blob-code blob-code-inner js-file-line">                                                        [<span class=pl-s>&#39;SHAPE@&#39;</span>, <span class=pl-s>&#39;Artificial&#39;</span>, <span class=pl-s>&#39;MODE_TYPE&#39;</span>, <span class=pl-s>&#39;MILES&#39;</span>, <span class=pl-s>&#39;TOT_UP_DWN&#39;</span>,</td>
      </tr>
      <tr>
        <td id="L757" class="blob-num js-line-number" data-line-number="757"></td>
        <td id="LC757" class="blob-code blob-code-inner js-file-line">                                                         <span class=pl-s>&#39;Volume&#39;</span>, <span class=pl-s>&#39;Capacity&#39;</span>, <span class=pl-s>&#39;VCR&#39;</span>])</td>
      </tr>
      <tr>
        <td id="L758" class="blob-num js-line-number" data-line-number="758"></td>
        <td id="LC758" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L759" class="blob-num js-line-number" data-line-number="759"></td>
        <td id="LC759" class="blob-code blob-code-inner js-file-line">                        <span class=pl-c># Insert new rows that include the mode-specific attributes</span></td>
      </tr>
      <tr>
        <td id="L760" class="blob-num js-line-number" data-line-number="760"></td>
        <td id="LC760" class="blob-code blob-code-inner js-file-line">                        <span class=pl-k>for</span> <span class=pl-s1>new_line</span> <span class=pl-c1>in</span> <span class=pl-s1>split_lines</span>:</td>
      </tr>
      <tr>
        <td id="L761" class="blob-num js-line-number" data-line-number="761"></td>
        <td id="LC761" class="blob-code blob-code-inner js-file-line">                            <span class=pl-s1>len_in_miles</span> <span class=pl-c1>=</span> <span class=pl-v>Q_</span>(<span class=pl-s1>new_line</span>.<span class=pl-s1>length</span>, <span class=pl-s>&quot;meters&quot;</span>).<span class=pl-en>to</span>(<span class=pl-s>&quot;miles&quot;</span>).<span class=pl-s1>magnitude</span></td>
      </tr>
      <tr>
        <td id="L762" class="blob-num js-line-number" data-line-number="762"></td>
        <td id="LC762" class="blob-code blob-code-inner js-file-line">                            <span class=pl-s1>icursor</span>.<span class=pl-en>insertRow</span>(</td>
      </tr>
      <tr>
        <td id="L763" class="blob-num js-line-number" data-line-number="763"></td>
        <td id="LC763" class="blob-code blob-code-inner js-file-line">                                [<span class=pl-s1>new_line</span>, <span class=pl-c1>0</span>, <span class=pl-s1>modal_layer_name</span>, <span class=pl-s1>len_in_miles</span>, <span class=pl-s1>in_tot_up_dwn</span>, <span class=pl-s1>in_volume</span>, <span class=pl-s1>in_capacity</span>,</td>
      </tr>
      <tr>
        <td id="L764" class="blob-num js-line-number" data-line-number="764"></td>
        <td id="LC764" class="blob-code blob-code-inner js-file-line">                                 <span class=pl-s1>in_vcr</span>])</td>
      </tr>
      <tr>
        <td id="L765" class="blob-num js-line-number" data-line-number="765"></td>
        <td id="LC765" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L766" class="blob-num js-line-number" data-line-number="766"></td>
        <td id="LC766" class="blob-code blob-code-inner js-file-line">                        <span class=pl-c># Delete cursor object</span></td>
      </tr>
      <tr>
        <td id="L767" class="blob-num js-line-number" data-line-number="767"></td>
        <td id="LC767" class="blob-code blob-code-inner js-file-line">                        <span class=pl-k>del</span> <span class=pl-s1>icursor</span></td>
      </tr>
      <tr>
        <td id="L768" class="blob-num js-line-number" data-line-number="768"></td>
        <td id="LC768" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L769" class="blob-num js-line-number" data-line-number="769"></td>
        <td id="LC769" class="blob-code blob-code-inner js-file-line">                    <span class=pl-k>else</span>:</td>
      </tr>
      <tr>
        <td id="L770" class="blob-num js-line-number" data-line-number="770"></td>
        <td id="LC770" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s1>logger</span>.<span class=pl-en>warning</span>(<span class=pl-s>&quot;Modal_layer_name: {} is not supported.&quot;</span>.<span class=pl-en>format</span>(<span class=pl-s1>modal_layer_name</span>))</td>
      </tr>
      <tr>
        <td id="L771" class="blob-num js-line-number" data-line-number="771"></td>
        <td id="LC771" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L772" class="blob-num js-line-number" data-line-number="772"></td>
        <td id="LC772" class="blob-code blob-code-inner js-file-line">                    <span class=pl-c># STEP 4:  Delete old unsplit data</span></td>
      </tr>
      <tr>
        <td id="L773" class="blob-num js-line-number" data-line-number="773"></td>
        <td id="LC773" class="blob-code blob-code-inner js-file-line">                    <span class=pl-k>with</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>UpdateCursor</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s1>modal_layer_name</span>), [<span class=pl-s>&#39;OID@&#39;</span>],</td>
      </tr>
      <tr>
        <td id="L774" class="blob-num js-line-number" data-line-number="774"></td>
        <td id="LC774" class="blob-code blob-code-inner js-file-line">                                               <span class=pl-s1>where_clause</span><span class=pl-c1>=</span><span class=pl-s1>id_fieldname</span> <span class=pl-c1>+</span> <span class=pl-s>&quot; = &quot;</span> <span class=pl-c1>+</span> <span class=pl-s1>theIdToGet</span>) <span class=pl-k>as</span> <span class=pl-s1>ucursor</span>:</td>
      </tr>
      <tr>
        <td id="L775" class="blob-num js-line-number" data-line-number="775"></td>
        <td id="LC775" class="blob-code blob-code-inner js-file-line">                        <span class=pl-k>for</span> <span class=pl-s1>row</span> <span class=pl-c1>in</span> <span class=pl-s1>ucursor</span>:</td>
      </tr>
      <tr>
        <td id="L776" class="blob-num js-line-number" data-line-number="776"></td>
        <td id="LC776" class="blob-code blob-code-inner js-file-line">                            <span class=pl-s1>ucursor</span>.<span class=pl-en>deleteRow</span>()</td>
      </tr>
      <tr>
        <td id="L777" class="blob-num js-line-number" data-line-number="777"></td>
        <td id="LC777" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L778" class="blob-num js-line-number" data-line-number="778"></td>
        <td id="LC778" class="blob-code blob-code-inner js-file-line">                <span class=pl-c># if the split doesn&#39;t work.</span></td>
      </tr>
      <tr>
        <td id="L779" class="blob-num js-line-number" data-line-number="779"></td>
        <td id="LC779" class="blob-code blob-code-inner js-file-line">                <span class=pl-k>else</span>:</td>
      </tr>
      <tr>
        <td id="L780" class="blob-num js-line-number" data-line-number="780"></td>
        <td id="LC780" class="blob-code blob-code-inner js-file-line">                    <span class=pl-s1>logger</span>.<span class=pl-en>detailed_debug</span>(</td>
      </tr>
      <tr>
        <td id="L781" class="blob-num js-line-number" data-line-number="781"></td>
        <td id="LC781" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s>&quot;the line split didn&#39;t work for ID: {}. &quot;</span></td>
      </tr>
      <tr>
        <td id="L782" class="blob-num js-line-number" data-line-number="782"></td>
        <td id="LC782" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s>&quot;Might want to investigate. &quot;</span></td>
      </tr>
      <tr>
        <td id="L783" class="blob-num js-line-number" data-line-number="783"></td>
        <td id="LC783" class="blob-code blob-code-inner js-file-line">                        <span class=pl-s>&quot;Could just be an artifact from the near result being the end of a line.&quot;</span>.<span class=pl-en>format</span>(</td>
      </tr>
      <tr>
        <td id="L784" class="blob-num js-line-number" data-line-number="784"></td>
        <td id="LC784" class="blob-code blob-code-inner js-file-line">                            <span class=pl-s1>theIdToGet</span>))</td>
      </tr>
      <tr>
        <td id="L785" class="blob-num js-line-number" data-line-number="785"></td>
        <td id="LC785" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L786" class="blob-num js-line-number" data-line-number="786"></td>
        <td id="LC786" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span>.<span class=pl-en>stopOperation</span>()</td>
      </tr>
      <tr>
        <td id="L787" class="blob-num js-line-number" data-line-number="787"></td>
        <td id="LC787" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span>.<span class=pl-en>stopEditing</span>(<span class=pl-c1>True</span>)</td>
      </tr>
      <tr>
        <td id="L788" class="blob-num js-line-number" data-line-number="788"></td>
        <td id="LC788" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L789" class="blob-num js-line-number" data-line-number="789"></td>
        <td id="LC789" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># delete the old features</span></td>
      </tr>
      <tr>
        <td id="L790" class="blob-num js-line-number" data-line-number="790"></td>
        <td id="LC790" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># ------------------------</span></td>
      </tr>
      <tr>
        <td id="L791" class="blob-num js-line-number" data-line-number="791"></td>
        <td id="LC791" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start:  delete old features (tmp_near, tmp_near_2, tmp_nodes)&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L792" class="blob-num js-line-number" data-line-number="792"></td>
        <td id="LC792" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>Exists</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_near&quot;</span>)):</td>
      </tr>
      <tr>
        <td id="L793" class="blob-num js-line-number" data-line-number="793"></td>
        <td id="LC793" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_near&quot;</span>))</td>
      </tr>
      <tr>
        <td id="L794" class="blob-num js-line-number" data-line-number="794"></td>
        <td id="LC794" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L795" class="blob-num js-line-number" data-line-number="795"></td>
        <td id="LC795" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>Exists</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_near_2&quot;</span>)):</td>
      </tr>
      <tr>
        <td id="L796" class="blob-num js-line-number" data-line-number="796"></td>
        <td id="LC796" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_near_2&quot;</span>))</td>
      </tr>
      <tr>
        <td id="L797" class="blob-num js-line-number" data-line-number="797"></td>
        <td id="LC797" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L798" class="blob-num js-line-number" data-line-number="798"></td>
        <td id="LC798" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>Exists</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_nodes&quot;</span>)):</td>
      </tr>
      <tr>
        <td id="L799" class="blob-num js-line-number" data-line-number="799"></td>
        <td id="LC799" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_nodes&quot;</span>))</td>
      </tr>
      <tr>
        <td id="L800" class="blob-num js-line-number" data-line-number="800"></td>
        <td id="LC800" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L801" class="blob-num js-line-number" data-line-number="801"></td>
        <td id="LC801" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># Add artificial links now.</span></td>
      </tr>
      <tr>
        <td id="L802" class="blob-num js-line-number" data-line-number="802"></td>
        <td id="LC802" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># now that the lines have been split add lines from the from points to the nearest node</span></td>
      </tr>
      <tr>
        <td id="L803" class="blob-num js-line-number" data-line-number="803"></td>
        <td id="LC803" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># --------------------------------------------------------------------------------------</span></td>
      </tr>
      <tr>
        <td id="L804" class="blob-num js-line-number" data-line-number="804"></td>
        <td id="LC804" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start:  add artificial links now w/ definition_query: {}&quot;</span>.<span class=pl-en>format</span>(<span class=pl-s1>definition_query</span>))</td>
      </tr>
      <tr>
        <td id="L805" class="blob-num js-line-number" data-line-number="805"></td>
        <td id="LC805" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start:  make_featurelayer 2&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L806" class="blob-num js-line-number" data-line-number="806"></td>
        <td id="LC806" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>fp_to_modal_layer</span> <span class=pl-c1>=</span> <span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;network&quot;</span>, <span class=pl-s1>modal_layer_name</span>)</td>
      </tr>
      <tr>
        <td id="L807" class="blob-num js-line-number" data-line-number="807"></td>
        <td id="LC807" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>MakeFeatureLayer_management</span>(<span class=pl-s1>fp_to_modal_layer</span>, <span class=pl-s>&quot;modal_lyr_&quot;</span> <span class=pl-c1>+</span> <span class=pl-s1>modal_layer_name</span> <span class=pl-c1>+</span> <span class=pl-s>&quot;2&quot;</span>, <span class=pl-s1>definition_query</span>)</td>
      </tr>
      <tr>
        <td id="L808" class="blob-num js-line-number" data-line-number="808"></td>
        <td id="LC808" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start:  feature vertices to points 2&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L809" class="blob-num js-line-number" data-line-number="809"></td>
        <td id="LC809" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>CheckProduct</span>(<span class=pl-s>&quot;ArcInfo&quot;</span>) <span class=pl-c1>==</span> <span class=pl-s>&quot;Available&quot;</span>:</td>
      </tr>
      <tr>
        <td id="L810" class="blob-num js-line-number" data-line-number="810"></td>
        <td id="LC810" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>FeatureVerticesToPoints_management</span>(<span class=pl-s1>in_features</span><span class=pl-c1>=</span><span class=pl-s>&quot;modal_lyr_&quot;</span> <span class=pl-c1>+</span> <span class=pl-s1>modal_layer_name</span> <span class=pl-c1>+</span> <span class=pl-s>&quot;2&quot;</span>,</td>
      </tr>
      <tr>
        <td id="L811" class="blob-num js-line-number" data-line-number="811"></td>
        <td id="LC811" class="blob-code blob-code-inner js-file-line">                                                 <span class=pl-s1>out_feature_class</span><span class=pl-c1>=</span><span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_nodes&quot;</span>),</td>
      </tr>
      <tr>
        <td id="L812" class="blob-num js-line-number" data-line-number="812"></td>
        <td id="LC812" class="blob-code blob-code-inner js-file-line">                                                 <span class=pl-s1>point_location</span><span class=pl-c1>=</span><span class=pl-s>&quot;BOTH_ENDS&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L813" class="blob-num js-line-number" data-line-number="813"></td>
        <td id="LC813" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>else</span>:</td>
      </tr>
      <tr>
        <td id="L814" class="blob-num js-line-number" data-line-number="814"></td>
        <td id="LC814" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>logger</span>.<span class=pl-en>warning</span>(<span class=pl-s>&quot;The Advanced/ArcInfo license level of ArcGIS is not available. Modified feature vertices &quot;</span></td>
      </tr>
      <tr>
        <td id="L815" class="blob-num js-line-number" data-line-number="815"></td>
        <td id="LC815" class="blob-code blob-code-inner js-file-line">                       <span class=pl-s>&quot;process will be run.&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L816" class="blob-num js-line-number" data-line-number="816"></td>
        <td id="LC816" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>AddGeometryAttributes_management</span>(<span class=pl-s>&quot;modal_lyr_&quot;</span> <span class=pl-c1>+</span> <span class=pl-s1>modal_layer_name</span> <span class=pl-c1>+</span> <span class=pl-s>&quot;2&quot;</span>, <span class=pl-s>&quot;LINE_START_MID_END&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L817" class="blob-num js-line-number" data-line-number="817"></td>
        <td id="LC817" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>MakeXYEventLayer_management</span>(<span class=pl-s>&quot;modal_lyr_&quot;</span> <span class=pl-c1>+</span> <span class=pl-s1>modal_layer_name</span> <span class=pl-c1>+</span> <span class=pl-s>&quot;2&quot;</span>, <span class=pl-s>&quot;START_X&quot;</span>, <span class=pl-s>&quot;START_Y&quot;</span>,</td>
      </tr>
      <tr>
        <td id="L818" class="blob-num js-line-number" data-line-number="818"></td>
        <td id="LC818" class="blob-code blob-code-inner js-file-line">                                          <span class=pl-s>&quot;modal_start_points_lyr&quot;</span>, <span class=pl-v>LCC_PROJ</span>)</td>
      </tr>
      <tr>
        <td id="L819" class="blob-num js-line-number" data-line-number="819"></td>
        <td id="LC819" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L820" class="blob-num js-line-number" data-line-number="820"></td>
        <td id="LC820" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>MakeXYEventLayer_management</span>(<span class=pl-s>&quot;modal_lyr_&quot;</span> <span class=pl-c1>+</span> <span class=pl-s1>modal_layer_name</span> <span class=pl-c1>+</span> <span class=pl-s>&quot;2&quot;</span>, <span class=pl-s>&quot;END_X&quot;</span>, <span class=pl-s>&quot;END_Y&quot;</span>,</td>
      </tr>
      <tr>
        <td id="L821" class="blob-num js-line-number" data-line-number="821"></td>
        <td id="LC821" class="blob-code blob-code-inner js-file-line">                                          <span class=pl-s>&quot;modal_end_points_lyr&quot;</span>, <span class=pl-v>LCC_PROJ</span>)</td>
      </tr>
      <tr>
        <td id="L822" class="blob-num js-line-number" data-line-number="822"></td>
        <td id="LC822" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L823" class="blob-num js-line-number" data-line-number="823"></td>
        <td id="LC823" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>FeatureClassToFeatureClass_conversion</span>(<span class=pl-s>&quot;modal_start_points_lyr&quot;</span>,</td>
      </tr>
      <tr>
        <td id="L824" class="blob-num js-line-number" data-line-number="824"></td>
        <td id="LC824" class="blob-code blob-code-inner js-file-line">                                                    <span class=pl-s1>scenario_gdb</span>,</td>
      </tr>
      <tr>
        <td id="L825" class="blob-num js-line-number" data-line-number="825"></td>
        <td id="LC825" class="blob-code blob-code-inner js-file-line">                                                    <span class=pl-s>&quot;tmp_nodes&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L826" class="blob-num js-line-number" data-line-number="826"></td>
        <td id="LC826" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L827" class="blob-num js-line-number" data-line-number="827"></td>
        <td id="LC827" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>Append_management</span>([<span class=pl-s>&quot;modal_end_points_lyr&quot;</span>],</td>
      </tr>
      <tr>
        <td id="L828" class="blob-num js-line-number" data-line-number="828"></td>
        <td id="LC828" class="blob-code blob-code-inner js-file-line">                                <span class=pl-s>&quot;tmp_nodes&quot;</span>, <span class=pl-s>&quot;NO_TEST&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L829" class="blob-num js-line-number" data-line-number="829"></td>
        <td id="LC829" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L830" class="blob-num js-line-number" data-line-number="830"></td>
        <td id="LC830" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s>&quot;modal_start_points_lyr&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L831" class="blob-num js-line-number" data-line-number="831"></td>
        <td id="LC831" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s>&quot;modal_end_points_lyr&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L832" class="blob-num js-line-number" data-line-number="832"></td>
        <td id="LC832" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>DeleteField_management</span>(<span class=pl-s1>fp_to_modal_layer</span>, <span class=pl-s>&quot;START_X&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L833" class="blob-num js-line-number" data-line-number="833"></td>
        <td id="LC833" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>DeleteField_management</span>(<span class=pl-s1>fp_to_modal_layer</span>, <span class=pl-s>&quot;START_Y&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L834" class="blob-num js-line-number" data-line-number="834"></td>
        <td id="LC834" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>DeleteField_management</span>(<span class=pl-s1>fp_to_modal_layer</span>, <span class=pl-s>&quot;MID_X&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L835" class="blob-num js-line-number" data-line-number="835"></td>
        <td id="LC835" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>DeleteField_management</span>(<span class=pl-s1>fp_to_modal_layer</span>, <span class=pl-s>&quot;MID_Y&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L836" class="blob-num js-line-number" data-line-number="836"></td>
        <td id="LC836" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>DeleteField_management</span>(<span class=pl-s1>fp_to_modal_layer</span>, <span class=pl-s>&quot;END_X&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L837" class="blob-num js-line-number" data-line-number="837"></td>
        <td id="LC837" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>DeleteField_management</span>(<span class=pl-s1>fp_to_modal_layer</span>, <span class=pl-s>&quot;END_Y&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L838" class="blob-num js-line-number" data-line-number="838"></td>
        <td id="LC838" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L839" class="blob-num js-line-number" data-line-number="839"></td>
        <td id="LC839" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start:  generate near table 2&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L840" class="blob-num js-line-number" data-line-number="840"></td>
        <td id="LC840" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>CheckProduct</span>(<span class=pl-s>&quot;ArcInfo&quot;</span>) <span class=pl-c1>==</span> <span class=pl-s>&quot;Available&quot;</span>:</td>
      </tr>
      <tr>
        <td id="L841" class="blob-num js-line-number" data-line-number="841"></td>
        <td id="LC841" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>GenerateNearTable_analysis</span>(<span class=pl-s1>locations_fc</span>, <span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_nodes&quot;</span>),</td>
      </tr>
      <tr>
        <td id="L842" class="blob-num js-line-number" data-line-number="842"></td>
        <td id="LC842" class="blob-code blob-code-inner js-file-line">                                         <span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_near_2&quot;</span>),</td>
      </tr>
      <tr>
        <td id="L843" class="blob-num js-line-number" data-line-number="843"></td>
        <td id="LC843" class="blob-code blob-code-inner js-file-line">                                         <span class=pl-s1>max_artificial_link_distance_miles</span>, <span class=pl-s>&quot;LOCATION&quot;</span>, <span class=pl-s>&quot;NO_ANGLE&quot;</span>, <span class=pl-s>&quot;CLOSEST&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L844" class="blob-num js-line-number" data-line-number="844"></td>
        <td id="LC844" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L845" class="blob-num js-line-number" data-line-number="845"></td>
        <td id="LC845" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>else</span>:</td>
      </tr>
      <tr>
        <td id="L846" class="blob-num js-line-number" data-line-number="846"></td>
        <td id="LC846" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>logger</span>.<span class=pl-en>warning</span>(<span class=pl-s>&quot;The Advanced/ArcInfo license level of ArcGIS is not available. Modified generate_near process &quot;</span></td>
      </tr>
      <tr>
        <td id="L847" class="blob-num js-line-number" data-line-number="847"></td>
        <td id="LC847" class="blob-code blob-code-inner js-file-line">                       <span class=pl-s>&quot;will be run.&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L848" class="blob-num js-line-number" data-line-number="848"></td>
        <td id="LC848" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># Spatial Join</span></td>
      </tr>
      <tr>
        <td id="L849" class="blob-num js-line-number" data-line-number="849"></td>
        <td id="LC849" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># Workaround for GenerateNearTable not being available for lower-level ArcGIS licenses.</span></td>
      </tr>
      <tr>
        <td id="L850" class="blob-num js-line-number" data-line-number="850"></td>
        <td id="LC850" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L851" class="blob-num js-line-number" data-line-number="851"></td>
        <td id="LC851" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>if</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>Exists</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_spatial_join_2&quot;</span>)):</td>
      </tr>
      <tr>
        <td id="L852" class="blob-num js-line-number" data-line-number="852"></td>
        <td id="LC852" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_spatial_join_2&quot;</span>))</td>
      </tr>
      <tr>
        <td id="L853" class="blob-num js-line-number" data-line-number="853"></td>
        <td id="LC853" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L854" class="blob-num js-line-number" data-line-number="854"></td>
        <td id="LC854" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># First, add field to capture joined FID</span></td>
      </tr>
      <tr>
        <td id="L855" class="blob-num js-line-number" data-line-number="855"></td>
        <td id="LC855" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s>&quot;tmp_nodes&quot;</span>, <span class=pl-s>&quot;Join_FID&quot;</span>, <span class=pl-s>&quot;LONG&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L856" class="blob-num js-line-number" data-line-number="856"></td>
        <td id="LC856" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>CalculateField_management</span>(<span class=pl-s>&quot;tmp_nodes&quot;</span>, <span class=pl-s>&quot;Join_FID&quot;</span>, <span class=pl-s>&quot;!OBJECTID!&quot;</span>, <span class=pl-s>&quot;PYTHON_9.3&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L857" class="blob-num js-line-number" data-line-number="857"></td>
        <td id="LC857" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L858" class="blob-num js-line-number" data-line-number="858"></td>
        <td id="LC858" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>SpatialJoin_analysis</span>(<span class=pl-s1>locations_fc</span>, <span class=pl-s>&quot;tmp_nodes&quot;</span>,</td>
      </tr>
      <tr>
        <td id="L859" class="blob-num js-line-number" data-line-number="859"></td>
        <td id="LC859" class="blob-code blob-code-inner js-file-line">                                   <span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_spatial_join_2&quot;</span>),</td>
      </tr>
      <tr>
        <td id="L860" class="blob-num js-line-number" data-line-number="860"></td>
        <td id="LC860" class="blob-code blob-code-inner js-file-line">                                   <span class=pl-s1>match_option</span><span class=pl-c1>=</span><span class=pl-s>&quot;CLOSEST&quot;</span>, <span class=pl-s1>search_radius</span><span class=pl-c1>=</span><span class=pl-s1>max_artificial_link_distance_miles</span>)</td>
      </tr>
      <tr>
        <td id="L861" class="blob-num js-line-number" data-line-number="861"></td>
        <td id="LC861" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L862" class="blob-num js-line-number" data-line-number="862"></td>
        <td id="LC862" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># queryPointAndDistance on the original point and corresponding spatial join match</span></td>
      </tr>
      <tr>
        <td id="L863" class="blob-num js-line-number" data-line-number="863"></td>
        <td id="LC863" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># For line in spatial_join:</span></td>
      </tr>
      <tr>
        <td id="L864" class="blob-num js-line-number" data-line-number="864"></td>
        <td id="LC864" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>result_dict</span> <span class=pl-c1>=</span> {}</td>
      </tr>
      <tr>
        <td id="L865" class="blob-num js-line-number" data-line-number="865"></td>
        <td id="LC865" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L866" class="blob-num js-line-number" data-line-number="866"></td>
        <td id="LC866" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>with</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>SearchCursor</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_spatial_join_2&quot;</span>),</td>
      </tr>
      <tr>
        <td id="L867" class="blob-num js-line-number" data-line-number="867"></td>
        <td id="LC867" class="blob-code blob-code-inner js-file-line">                                   [<span class=pl-s>&quot;Target_FID&quot;</span>, <span class=pl-s>&quot;Join_FID&quot;</span>, <span class=pl-s>&quot;SHAPE@&quot;</span>]) <span class=pl-k>as</span> <span class=pl-s1>scursor1</span>:</td>
      </tr>
      <tr>
        <td id="L868" class="blob-num js-line-number" data-line-number="868"></td>
        <td id="LC868" class="blob-code blob-code-inner js-file-line">            <span class=pl-k>for</span> <span class=pl-s1>row1</span> <span class=pl-c1>in</span> <span class=pl-s1>scursor1</span>:</td>
      </tr>
      <tr>
        <td id="L869" class="blob-num js-line-number" data-line-number="869"></td>
        <td id="LC869" class="blob-code blob-code-inner js-file-line">                <span class=pl-k>with</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>SearchCursor</span>(<span class=pl-s>&quot;tmp_nodes&quot;</span>,</td>
      </tr>
      <tr>
        <td id="L870" class="blob-num js-line-number" data-line-number="870"></td>
        <td id="LC870" class="blob-code blob-code-inner js-file-line">                                           [<span class=pl-s>&quot;OBJECTID&quot;</span>, <span class=pl-s>&quot;SHAPE@&quot;</span>]) <span class=pl-k>as</span> <span class=pl-s1>scursor2</span>:</td>
      </tr>
      <tr>
        <td id="L871" class="blob-num js-line-number" data-line-number="871"></td>
        <td id="LC871" class="blob-code blob-code-inner js-file-line">                    <span class=pl-k>for</span> <span class=pl-s1>row2</span> <span class=pl-c1>in</span> <span class=pl-s1>scursor2</span>:</td>
      </tr>
      <tr>
        <td id="L872" class="blob-num js-line-number" data-line-number="872"></td>
        <td id="LC872" class="blob-code blob-code-inner js-file-line">                        <span class=pl-k>if</span> <span class=pl-s1>row1</span>[<span class=pl-c1>1</span>] <span class=pl-c1>==</span> <span class=pl-s1>row2</span>[<span class=pl-c1>0</span>]:</td>
      </tr>
      <tr>
        <td id="L873" class="blob-num js-line-number" data-line-number="873"></td>
        <td id="LC873" class="blob-code blob-code-inner js-file-line">                            <span class=pl-s1>result</span> <span class=pl-c1>=</span> <span class=pl-s1>row2</span>[<span class=pl-c1>1</span>].<span class=pl-en>angleAndDistanceTo</span>(<span class=pl-s1>row1</span>[<span class=pl-c1>2</span>], <span class=pl-s>&quot;PLANAR&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L874" class="blob-num js-line-number" data-line-number="874"></td>
        <td id="LC874" class="blob-code blob-code-inner js-file-line">                            <span class=pl-c># Capture the point geometry of the nearest point on the polyline to the location point</span></td>
      </tr>
      <tr>
        <td id="L875" class="blob-num js-line-number" data-line-number="875"></td>
        <td id="LC875" class="blob-code blob-code-inner js-file-line">                            <span class=pl-c># and the minimum distance between the line and the point</span></td>
      </tr>
      <tr>
        <td id="L876" class="blob-num js-line-number" data-line-number="876"></td>
        <td id="LC876" class="blob-code blob-code-inner js-file-line">                            <span class=pl-c># result_dict[in_fid] = [near_fid, from_xy, near_xy, near_dist</span></td>
      </tr>
      <tr>
        <td id="L877" class="blob-num js-line-number" data-line-number="877"></td>
        <td id="LC877" class="blob-code blob-code-inner js-file-line">                            <span class=pl-s1>result_dict</span>[<span class=pl-s1>row1</span>[<span class=pl-c1>0</span>]] <span class=pl-c1>=</span> [<span class=pl-s1>row1</span>[<span class=pl-c1>1</span>], <span class=pl-s1>row1</span>[<span class=pl-c1>2</span>], <span class=pl-s1>row2</span>[<span class=pl-c1>1</span>], <span class=pl-s1>result</span>[<span class=pl-c1>1</span>]]</td>
      </tr>
      <tr>
        <td id="L878" class="blob-num js-line-number" data-line-number="878"></td>
        <td id="LC878" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L879" class="blob-num js-line-number" data-line-number="879"></td>
        <td id="LC879" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># Write to a tmp_near table equivalent to what is create by Generate Near Table tool</span></td>
      </tr>
      <tr>
        <td id="L880" class="blob-num js-line-number" data-line-number="880"></td>
        <td id="LC880" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>CreateTable_management</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_near_2&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L881" class="blob-num js-line-number" data-line-number="881"></td>
        <td id="LC881" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s>&quot;tmp_near_2&quot;</span>, <span class=pl-s>&quot;IN_FID&quot;</span>, <span class=pl-s>&quot;LONG&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L882" class="blob-num js-line-number" data-line-number="882"></td>
        <td id="LC882" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s>&quot;tmp_near_2&quot;</span>, <span class=pl-s>&quot;NEAR_FID&quot;</span>, <span class=pl-s>&quot;LONG&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L883" class="blob-num js-line-number" data-line-number="883"></td>
        <td id="LC883" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s>&quot;tmp_near_2&quot;</span>, <span class=pl-s>&quot;NEAR_DIST&quot;</span>, <span class=pl-s>&quot;LONG&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L884" class="blob-num js-line-number" data-line-number="884"></td>
        <td id="LC884" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s>&quot;tmp_near_2&quot;</span>, <span class=pl-s>&quot;FROM_X&quot;</span>, <span class=pl-s>&quot;DOUBLE&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L885" class="blob-num js-line-number" data-line-number="885"></td>
        <td id="LC885" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s>&quot;tmp_near_2&quot;</span>, <span class=pl-s>&quot;FROM_Y&quot;</span>, <span class=pl-s>&quot;DOUBLE&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L886" class="blob-num js-line-number" data-line-number="886"></td>
        <td id="LC886" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s>&quot;tmp_near_2&quot;</span>, <span class=pl-s>&quot;NEAR_X&quot;</span>, <span class=pl-s>&quot;DOUBLE&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L887" class="blob-num js-line-number" data-line-number="887"></td>
        <td id="LC887" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s>&quot;tmp_near_2&quot;</span>, <span class=pl-s>&quot;NEAR_Y&quot;</span>, <span class=pl-s>&quot;DOUBLE&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L888" class="blob-num js-line-number" data-line-number="888"></td>
        <td id="LC888" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L889" class="blob-num js-line-number" data-line-number="889"></td>
        <td id="LC889" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># insert the relevant data into the table</span></td>
      </tr>
      <tr>
        <td id="L890" class="blob-num js-line-number" data-line-number="890"></td>
        <td id="LC890" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>icursor</span> <span class=pl-c1>=</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>InsertCursor</span>(<span class=pl-s>&quot;tmp_near_2&quot;</span>, [<span class=pl-s>&#39;IN_FID&#39;</span>, <span class=pl-s>&#39;NEAR_FID&#39;</span>, <span class=pl-s>&#39;NEAR_DIST&#39;</span>, <span class=pl-s>&#39;FROM_X&#39;</span>, <span class=pl-s>&#39;FROM_Y&#39;</span>,</td>
      </tr>
      <tr>
        <td id="L891" class="blob-num js-line-number" data-line-number="891"></td>
        <td id="LC891" class="blob-code blob-code-inner js-file-line">                                                       <span class=pl-s>&#39;NEAR_X&#39;</span>, <span class=pl-s>&#39;NEAR_Y&#39;</span>])</td>
      </tr>
      <tr>
        <td id="L892" class="blob-num js-line-number" data-line-number="892"></td>
        <td id="LC892" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L893" class="blob-num js-line-number" data-line-number="893"></td>
        <td id="LC893" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>for</span> <span class=pl-s1>in_fid</span> <span class=pl-c1>in</span> <span class=pl-s1>result_dict</span>:</td>
      </tr>
      <tr>
        <td id="L894" class="blob-num js-line-number" data-line-number="894"></td>
        <td id="LC894" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>near_fid</span> <span class=pl-c1>=</span> <span class=pl-s1>result_dict</span>[<span class=pl-s1>in_fid</span>][<span class=pl-c1>0</span>]</td>
      </tr>
      <tr>
        <td id="L895" class="blob-num js-line-number" data-line-number="895"></td>
        <td id="LC895" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>near_distance</span> <span class=pl-c1>=</span> <span class=pl-s1>result_dict</span>[<span class=pl-s1>in_fid</span>][<span class=pl-c1>3</span>]</td>
      </tr>
      <tr>
        <td id="L896" class="blob-num js-line-number" data-line-number="896"></td>
        <td id="LC896" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>from_x</span> <span class=pl-c1>=</span> <span class=pl-s1>result_dict</span>[<span class=pl-s1>in_fid</span>][<span class=pl-c1>1</span>].<span class=pl-s1>firstPoint</span>.<span class=pl-v>X</span></td>
      </tr>
      <tr>
        <td id="L897" class="blob-num js-line-number" data-line-number="897"></td>
        <td id="LC897" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>from_y</span> <span class=pl-c1>=</span> <span class=pl-s1>result_dict</span>[<span class=pl-s1>in_fid</span>][<span class=pl-c1>1</span>].<span class=pl-s1>firstPoint</span>.<span class=pl-v>Y</span></td>
      </tr>
      <tr>
        <td id="L898" class="blob-num js-line-number" data-line-number="898"></td>
        <td id="LC898" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>near_x</span> <span class=pl-c1>=</span> <span class=pl-s1>result_dict</span>[<span class=pl-s1>in_fid</span>][<span class=pl-c1>2</span>].<span class=pl-s1>firstPoint</span>.<span class=pl-v>X</span></td>
      </tr>
      <tr>
        <td id="L899" class="blob-num js-line-number" data-line-number="899"></td>
        <td id="LC899" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>near_y</span> <span class=pl-c1>=</span> <span class=pl-s1>result_dict</span>[<span class=pl-s1>in_fid</span>][<span class=pl-c1>2</span>].<span class=pl-s1>firstPoint</span>.<span class=pl-v>Y</span></td>
      </tr>
      <tr>
        <td id="L900" class="blob-num js-line-number" data-line-number="900"></td>
        <td id="LC900" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>icursor</span>.<span class=pl-en>insertRow</span>([<span class=pl-s1>in_fid</span>, <span class=pl-s1>near_fid</span>, <span class=pl-s1>near_distance</span>, <span class=pl-s1>from_x</span>, <span class=pl-s1>from_y</span>, <span class=pl-s1>near_x</span>, <span class=pl-s1>near_y</span>])</td>
      </tr>
      <tr>
        <td id="L901" class="blob-num js-line-number" data-line-number="901"></td>
        <td id="LC901" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L902" class="blob-num js-line-number" data-line-number="902"></td>
        <td id="LC902" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>del</span> <span class=pl-s1>icursor</span></td>
      </tr>
      <tr>
        <td id="L903" class="blob-num js-line-number" data-line-number="903"></td>
        <td id="LC903" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L904" class="blob-num js-line-number" data-line-number="904"></td>
        <td id="LC904" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_spatial_join_2&quot;</span>))</td>
      </tr>
      <tr>
        <td id="L905" class="blob-num js-line-number" data-line-number="905"></td>
        <td id="LC905" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L906" class="blob-num js-line-number" data-line-number="906"></td>
        <td id="LC906" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start:  delete tmp_nodes&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L907" class="blob-num js-line-number" data-line-number="907"></td>
        <td id="LC907" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_nodes&quot;</span>))</td>
      </tr>
      <tr>
        <td id="L908" class="blob-num js-line-number" data-line-number="908"></td>
        <td id="LC908" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L909" class="blob-num js-line-number" data-line-number="909"></td>
        <td id="LC909" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start:  start editor&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L910" class="blob-num js-line-number" data-line-number="910"></td>
        <td id="LC910" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span> <span class=pl-c1>=</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>Editor</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>))</td>
      </tr>
      <tr>
        <td id="L911" class="blob-num js-line-number" data-line-number="911"></td>
        <td id="LC911" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span>.<span class=pl-en>startEditing</span>(<span class=pl-c1>False</span>, <span class=pl-c1>False</span>)</td>
      </tr>
      <tr>
        <td id="L912" class="blob-num js-line-number" data-line-number="912"></td>
        <td id="LC912" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span>.<span class=pl-en>startOperation</span>()</td>
      </tr>
      <tr>
        <td id="L913" class="blob-num js-line-number" data-line-number="913"></td>
        <td id="LC913" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L914" class="blob-num js-line-number" data-line-number="914"></td>
        <td id="LC914" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>icursor</span> <span class=pl-c1>=</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>InsertCursor</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s1>modal_layer_name</span>),</td>
      </tr>
      <tr>
        <td id="L915" class="blob-num js-line-number" data-line-number="915"></td>
        <td id="LC915" class="blob-code blob-code-inner js-file-line">                                    [<span class=pl-s>&#39;SHAPE@&#39;</span>, <span class=pl-s>&#39;Artificial&#39;</span>, <span class=pl-s>&#39;MODE_TYPE&#39;</span>, <span class=pl-s>&#39;MILES&#39;</span>, <span class=pl-s>&#39;LOCATION_ID&#39;</span>,</td>
      </tr>
      <tr>
        <td id="L916" class="blob-num js-line-number" data-line-number="916"></td>
        <td id="LC916" class="blob-code blob-code-inner js-file-line">                                     <span class=pl-s>&#39;LOCATION_ID_NAME&#39;</span>])  <span class=pl-c># add location_id for setting flow restrictions</span></td>
      </tr>
      <tr>
        <td id="L917" class="blob-num js-line-number" data-line-number="917"></td>
        <td id="LC917" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L918" class="blob-num js-line-number" data-line-number="918"></td>
        <td id="LC918" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>location_id_name_dict</span> <span class=pl-c1>=</span> <span class=pl-en>get_location_id_name_dict</span>(<span class=pl-s1>the_scenario</span>, <span class=pl-s1>logger</span>)</td>
      </tr>
      <tr>
        <td id="L919" class="blob-num js-line-number" data-line-number="919"></td>
        <td id="LC919" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>connected_location_ids</span> <span class=pl-c1>=</span> []</td>
      </tr>
      <tr>
        <td id="L920" class="blob-num js-line-number" data-line-number="920"></td>
        <td id="LC920" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>connected_location_id_names</span> <span class=pl-c1>=</span> []</td>
      </tr>
      <tr>
        <td id="L921" class="blob-num js-line-number" data-line-number="921"></td>
        <td id="LC921" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start:  search cursor on tmp_near_2&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L922" class="blob-num js-line-number" data-line-number="922"></td>
        <td id="LC922" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>with</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>SearchCursor</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_near_2&quot;</span>),</td>
      </tr>
      <tr>
        <td id="L923" class="blob-num js-line-number" data-line-number="923"></td>
        <td id="LC923" class="blob-code blob-code-inner js-file-line">                               [<span class=pl-s>&quot;FROM_X&quot;</span>, <span class=pl-s>&quot;FROM_Y&quot;</span>, <span class=pl-s>&quot;NEAR_X&quot;</span>, <span class=pl-s>&quot;NEAR_Y&quot;</span>, <span class=pl-s>&quot;NEAR_DIST&quot;</span>, <span class=pl-s>&quot;IN_FID&quot;</span>]) <span class=pl-k>as</span> <span class=pl-s1>scursor</span>:</td>
      </tr>
      <tr>
        <td id="L924" class="blob-num js-line-number" data-line-number="924"></td>
        <td id="LC924" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L925" class="blob-num js-line-number" data-line-number="925"></td>
        <td id="LC925" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>for</span> <span class=pl-s1>row</span> <span class=pl-c1>in</span> <span class=pl-s1>scursor</span>:</td>
      </tr>
      <tr>
        <td id="L926" class="blob-num js-line-number" data-line-number="926"></td>
        <td id="LC926" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L927" class="blob-num js-line-number" data-line-number="927"></td>
        <td id="LC927" class="blob-code blob-code-inner js-file-line">            <span class=pl-k>if</span> <span class=pl-c1>not</span> <span class=pl-s1>row</span>[<span class=pl-c1>4</span>] <span class=pl-c1>==</span> <span class=pl-c1>0</span>:</td>
      </tr>
      <tr>
        <td id="L928" class="blob-num js-line-number" data-line-number="928"></td>
        <td id="LC928" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L929" class="blob-num js-line-number" data-line-number="929"></td>
        <td id="LC929" class="blob-code blob-code-inner js-file-line">                <span class=pl-c># use the unique objectid (in_fid) from the near to determine</span></td>
      </tr>
      <tr>
        <td id="L930" class="blob-num js-line-number" data-line-number="930"></td>
        <td id="LC930" class="blob-code blob-code-inner js-file-line">                <span class=pl-c># if we have an in or an out location.</span></td>
      </tr>
      <tr>
        <td id="L931" class="blob-num js-line-number" data-line-number="931"></td>
        <td id="LC931" class="blob-code blob-code-inner js-file-line">                <span class=pl-c># then set the flow restrictions appropriately.</span></td>
      </tr>
      <tr>
        <td id="L932" class="blob-num js-line-number" data-line-number="932"></td>
        <td id="LC932" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L933" class="blob-num js-line-number" data-line-number="933"></td>
        <td id="LC933" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>in_fid</span> <span class=pl-c1>=</span> <span class=pl-s1>row</span>[<span class=pl-c1>5</span>]</td>
      </tr>
      <tr>
        <td id="L934" class="blob-num js-line-number" data-line-number="934"></td>
        <td id="LC934" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>location_id_name</span> <span class=pl-c1>=</span> <span class=pl-s1>location_id_name_dict</span>[<span class=pl-s1>in_fid</span>]</td>
      </tr>
      <tr>
        <td id="L935" class="blob-num js-line-number" data-line-number="935"></td>
        <td id="LC935" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>location_id</span> <span class=pl-c1>=</span> <span class=pl-s1>location_id_name</span>.<span class=pl-en>split</span>(<span class=pl-s>&quot;_&quot;</span>)[<span class=pl-c1>0</span>]</td>
      </tr>
      <tr>
        <td id="L936" class="blob-num js-line-number" data-line-number="936"></td>
        <td id="LC936" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>connected_location_ids</span>.<span class=pl-en>append</span>(<span class=pl-s1>location_id</span>)</td>
      </tr>
      <tr>
        <td id="L937" class="blob-num js-line-number" data-line-number="937"></td>
        <td id="LC937" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>connected_location_id_names</span>.<span class=pl-en>append</span>(<span class=pl-s1>location_id_name</span>)</td>
      </tr>
      <tr>
        <td id="L938" class="blob-num js-line-number" data-line-number="938"></td>
        <td id="LC938" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L939" class="blob-num js-line-number" data-line-number="939"></td>
        <td id="LC939" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>coordList</span> <span class=pl-c1>=</span> []</td>
      </tr>
      <tr>
        <td id="L940" class="blob-num js-line-number" data-line-number="940"></td>
        <td id="LC940" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>coordList</span>.<span class=pl-en>append</span>(<span class=pl-s1>arcpy</span>.<span class=pl-v>Point</span>(<span class=pl-s1>row</span>[<span class=pl-c1>0</span>], <span class=pl-s1>row</span>[<span class=pl-c1>1</span>]))</td>
      </tr>
      <tr>
        <td id="L941" class="blob-num js-line-number" data-line-number="941"></td>
        <td id="LC941" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>coordList</span>.<span class=pl-en>append</span>(<span class=pl-s1>arcpy</span>.<span class=pl-v>Point</span>(<span class=pl-s1>row</span>[<span class=pl-c1>2</span>], <span class=pl-s1>row</span>[<span class=pl-c1>3</span>]))</td>
      </tr>
      <tr>
        <td id="L942" class="blob-num js-line-number" data-line-number="942"></td>
        <td id="LC942" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>polyline</span> <span class=pl-c1>=</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>Polyline</span>(<span class=pl-s1>arcpy</span>.<span class=pl-v>Array</span>(<span class=pl-s1>coordList</span>))</td>
      </tr>
      <tr>
        <td id="L943" class="blob-num js-line-number" data-line-number="943"></td>
        <td id="LC943" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L944" class="blob-num js-line-number" data-line-number="944"></td>
        <td id="LC944" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>len_in_miles</span> <span class=pl-c1>=</span> <span class=pl-v>Q_</span>(<span class=pl-s1>polyline</span>.<span class=pl-s1>length</span>, <span class=pl-s>&quot;meters&quot;</span>).<span class=pl-en>to</span>(<span class=pl-s>&quot;miles&quot;</span>).<span class=pl-s1>magnitude</span></td>
      </tr>
      <tr>
        <td id="L945" class="blob-num js-line-number" data-line-number="945"></td>
        <td id="LC945" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L946" class="blob-num js-line-number" data-line-number="946"></td>
        <td id="LC946" class="blob-code blob-code-inner js-file-line">                <span class=pl-c># insert artificial link attributes</span></td>
      </tr>
      <tr>
        <td id="L947" class="blob-num js-line-number" data-line-number="947"></td>
        <td id="LC947" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>icursor</span>.<span class=pl-en>insertRow</span>([<span class=pl-s1>polyline</span>, <span class=pl-c1>1</span>, <span class=pl-s1>modal_layer_name</span>, <span class=pl-s1>len_in_miles</span>, <span class=pl-s1>location_id</span>, <span class=pl-s1>location_id_name</span>])</td>
      </tr>
      <tr>
        <td id="L948" class="blob-num js-line-number" data-line-number="948"></td>
        <td id="LC948" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L949" class="blob-num js-line-number" data-line-number="949"></td>
        <td id="LC949" class="blob-code blob-code-inner js-file-line">            <span class=pl-k>else</span>:</td>
      </tr>
      <tr>
        <td id="L950" class="blob-num js-line-number" data-line-number="950"></td>
        <td id="LC950" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>logger</span>.<span class=pl-en>warning</span>(<span class=pl-s>&quot;Artificial Link code: Ignoring NEAR_FID {} with NEAR_DIST {}&quot;</span>.<span class=pl-en>format</span>(<span class=pl-s1>row</span>[<span class=pl-c1>0</span>], <span class=pl-s1>row</span>[<span class=pl-c1>4</span>]))</td>
      </tr>
      <tr>
        <td id="L951" class="blob-num js-line-number" data-line-number="951"></td>
        <td id="LC951" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L952" class="blob-num js-line-number" data-line-number="952"></td>
        <td id="LC952" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>del</span> <span class=pl-s1>icursor</span></td>
      </tr>
      <tr>
        <td id="L953" class="blob-num js-line-number" data-line-number="953"></td>
        <td id="LC953" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start:  stop editing&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L954" class="blob-num js-line-number" data-line-number="954"></td>
        <td id="LC954" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span>.<span class=pl-en>stopOperation</span>()</td>
      </tr>
      <tr>
        <td id="L955" class="blob-num js-line-number" data-line-number="955"></td>
        <td id="LC955" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span>.<span class=pl-en>stopEditing</span>(<span class=pl-c1>True</span>)</td>
      </tr>
      <tr>
        <td id="L956" class="blob-num js-line-number" data-line-number="956"></td>
        <td id="LC956" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L957" class="blob-num js-line-number" data-line-number="957"></td>
        <td id="LC957" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s>&quot;tmp_near_2&quot;</span>))</td>
      </tr>
      <tr>
        <td id="L958" class="blob-num js-line-number" data-line-number="958"></td>
        <td id="LC958" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L959" class="blob-num js-line-number" data-line-number="959"></td>
        <td id="LC959" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># ALSO SET CONNECTS_X FIELD IN POINT LAYER</span></td>
      </tr>
      <tr>
        <td id="L960" class="blob-num js-line-number" data-line-number="960"></td>
        <td id="LC960" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># -----------------------------------------</span></td>
      </tr>
      <tr>
        <td id="L961" class="blob-num js-line-number" data-line-number="961"></td>
        <td id="LC961" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;start:  connect_x&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L962" class="blob-num js-line-number" data-line-number="962"></td>
        <td id="LC962" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>AddField_management</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s1>locations_fc</span>), <span class=pl-s>&quot;connects_&quot;</span> <span class=pl-c1>+</span> <span class=pl-s1>modal_layer_name</span>, <span class=pl-s>&quot;SHORT&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L963" class="blob-num js-line-number" data-line-number="963"></td>
        <td id="LC963" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>CalculateField_management</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s1>locations_fc</span>), <span class=pl-s>&quot;connects_&quot;</span> <span class=pl-c1>+</span> <span class=pl-s1>modal_layer_name</span>, <span class=pl-c1>0</span>,</td>
      </tr>
      <tr>
        <td id="L964" class="blob-num js-line-number" data-line-number="964"></td>
        <td id="LC964" class="blob-code blob-code-inner js-file-line">                                    <span class=pl-s>&quot;PYTHON_9.3&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L965" class="blob-num js-line-number" data-line-number="965"></td>
        <td id="LC965" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L966" class="blob-num js-line-number" data-line-number="966"></td>
        <td id="LC966" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span> <span class=pl-c1>=</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>Editor</span>(<span class=pl-s1>scenario_gdb</span>)</td>
      </tr>
      <tr>
        <td id="L967" class="blob-num js-line-number" data-line-number="967"></td>
        <td id="LC967" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span>.<span class=pl-en>startEditing</span>(<span class=pl-c1>False</span>, <span class=pl-c1>False</span>)</td>
      </tr>
      <tr>
        <td id="L968" class="blob-num js-line-number" data-line-number="968"></td>
        <td id="LC968" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span>.<span class=pl-en>startOperation</span>()</td>
      </tr>
      <tr>
        <td id="L969" class="blob-num js-line-number" data-line-number="969"></td>
        <td id="LC969" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>with</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>UpdateCursor</span>(<span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>join</span>(<span class=pl-s1>scenario_gdb</span>, <span class=pl-s1>locations_fc</span>),</td>
      </tr>
      <tr>
        <td id="L970" class="blob-num js-line-number" data-line-number="970"></td>
        <td id="LC970" class="blob-code blob-code-inner js-file-line">                               [<span class=pl-s>&quot;LOCATION_ID_NAME&quot;</span>, <span class=pl-s>&quot;connects_&quot;</span> <span class=pl-c1>+</span> <span class=pl-s1>modal_layer_name</span>]) <span class=pl-k>as</span> <span class=pl-s1>cursor</span>:</td>
      </tr>
      <tr>
        <td id="L971" class="blob-num js-line-number" data-line-number="971"></td>
        <td id="LC971" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L972" class="blob-num js-line-number" data-line-number="972"></td>
        <td id="LC972" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>for</span> <span class=pl-s1>row</span> <span class=pl-c1>in</span> <span class=pl-s1>cursor</span>:</td>
      </tr>
      <tr>
        <td id="L973" class="blob-num js-line-number" data-line-number="973"></td>
        <td id="LC973" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L974" class="blob-num js-line-number" data-line-number="974"></td>
        <td id="LC974" class="blob-code blob-code-inner js-file-line">            <span class=pl-k>if</span> <span class=pl-s1>row</span>[<span class=pl-c1>0</span>] <span class=pl-c1>in</span> <span class=pl-s1>connected_location_id_names</span>:</td>
      </tr>
      <tr>
        <td id="L975" class="blob-num js-line-number" data-line-number="975"></td>
        <td id="LC975" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>row</span>[<span class=pl-c1>1</span>] <span class=pl-c1>=</span> <span class=pl-c1>1</span></td>
      </tr>
      <tr>
        <td id="L976" class="blob-num js-line-number" data-line-number="976"></td>
        <td id="LC976" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>cursor</span>.<span class=pl-en>updateRow</span>(<span class=pl-s1>row</span>)</td>
      </tr>
      <tr>
        <td id="L977" class="blob-num js-line-number" data-line-number="977"></td>
        <td id="LC977" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L978" class="blob-num js-line-number" data-line-number="978"></td>
        <td id="LC978" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span>.<span class=pl-en>stopOperation</span>()</td>
      </tr>
      <tr>
        <td id="L979" class="blob-num js-line-number" data-line-number="979"></td>
        <td id="LC979" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span>.<span class=pl-en>stopEditing</span>(<span class=pl-c1>True</span>)</td>
      </tr>
      <tr>
        <td id="L980" class="blob-num js-line-number" data-line-number="980"></td>
        <td id="LC980" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L981" class="blob-num js-line-number" data-line-number="981"></td>
        <td id="LC981" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;finish: locations_add_links&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L982" class="blob-num js-line-number" data-line-number="982"></td>
        <td id="LC982" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L983" class="blob-num js-line-number" data-line-number="983"></td>
        <td id="LC983" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L984" class="blob-num js-line-number" data-line-number="984"></td>
        <td id="LC984" class="blob-code blob-code-inner js-file-line"><span class=pl-c># ==============================================================================</span></td>
      </tr>
      <tr>
        <td id="L985" class="blob-num js-line-number" data-line-number="985"></td>
        <td id="LC985" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L986" class="blob-num js-line-number" data-line-number="986"></td>
        <td id="LC986" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L987" class="blob-num js-line-number" data-line-number="987"></td>
        <td id="LC987" class="blob-code blob-code-inner js-file-line"><span class=pl-k>def</span> <span class=pl-en>ignore_locations_not_connected_to_network</span>(<span class=pl-s1>the_scenario</span>, <span class=pl-s1>logger</span>):</td>
      </tr>
      <tr>
        <td id="L988" class="blob-num js-line-number" data-line-number="988"></td>
        <td id="LC988" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>info</span>(<span class=pl-s>&quot;start: ignore_locations_not_connected_to_network&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L989" class="blob-num js-line-number" data-line-number="989"></td>
        <td id="LC989" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;flag locations which don&#39;t connect to the network&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L990" class="blob-num js-line-number" data-line-number="990"></td>
        <td id="LC990" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L991" class="blob-num js-line-number" data-line-number="991"></td>
        <td id="LC991" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># flag locations which dont connect to the network in the GIS</span></td>
      </tr>
      <tr>
        <td id="L992" class="blob-num js-line-number" data-line-number="992"></td>
        <td id="LC992" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># -----------------------------------------------------------</span></td>
      </tr>
      <tr>
        <td id="L993" class="blob-num js-line-number" data-line-number="993"></td>
        <td id="LC993" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L994" class="blob-num js-line-number" data-line-number="994"></td>
        <td id="LC994" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># add the ignore field to the fc</span></td>
      </tr>
      <tr>
        <td id="L995" class="blob-num js-line-number" data-line-number="995"></td>
        <td id="LC995" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>scenario_gdb</span> <span class=pl-c1>=</span> <span class=pl-s1>the_scenario</span>.<span class=pl-s1>main_gdb</span></td>
      </tr>
      <tr>
        <td id="L996" class="blob-num js-line-number" data-line-number="996"></td>
        <td id="LC996" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>locations_fc</span> <span class=pl-c1>=</span> <span class=pl-s1>the_scenario</span>.<span class=pl-s1>locations_fc</span></td>
      </tr>
      <tr>
        <td id="L997" class="blob-num js-line-number" data-line-number="997"></td>
        <td id="LC997" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L998" class="blob-num js-line-number" data-line-number="998"></td>
        <td id="LC998" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span> <span class=pl-c1>=</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>Editor</span>(<span class=pl-s1>scenario_gdb</span>)</td>
      </tr>
      <tr>
        <td id="L999" class="blob-num js-line-number" data-line-number="999"></td>
        <td id="LC999" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span>.<span class=pl-en>startEditing</span>(<span class=pl-c1>False</span>, <span class=pl-c1>False</span>)</td>
      </tr>
      <tr>
        <td id="L1000" class="blob-num js-line-number" data-line-number="1000"></td>
        <td id="LC1000" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span>.<span class=pl-en>startOperation</span>()</td>
      </tr>
      <tr>
        <td id="L1001" class="blob-num js-line-number" data-line-number="1001"></td>
        <td id="LC1001" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1002" class="blob-num js-line-number" data-line-number="1002"></td>
        <td id="LC1002" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>query</span> <span class=pl-c1>=</span> <span class=pl-s>&quot;connects_road = 0 and &quot;</span> \</td>
      </tr>
      <tr>
        <td id="L1003" class="blob-num js-line-number" data-line-number="1003"></td>
        <td id="LC1003" class="blob-code blob-code-inner js-file-line">            <span class=pl-s>&quot;connects_rail = 0 and &quot;</span> \</td>
      </tr>
      <tr>
        <td id="L1004" class="blob-num js-line-number" data-line-number="1004"></td>
        <td id="LC1004" class="blob-code blob-code-inner js-file-line">            <span class=pl-s>&quot;connects_water = 0 and &quot;</span> \</td>
      </tr>
      <tr>
        <td id="L1005" class="blob-num js-line-number" data-line-number="1005"></td>
        <td id="LC1005" class="blob-code blob-code-inner js-file-line">            <span class=pl-s>&quot;connects_pipeline_prod_trf_rts = 0 and &quot;</span> \</td>
      </tr>
      <tr>
        <td id="L1006" class="blob-num js-line-number" data-line-number="1006"></td>
        <td id="LC1006" class="blob-code blob-code-inner js-file-line">            <span class=pl-s>&quot;connects_pipeline_crude_trf_rts = 0&quot;</span></td>
      </tr>
      <tr>
        <td id="L1007" class="blob-num js-line-number" data-line-number="1007"></td>
        <td id="LC1007" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1008" class="blob-num js-line-number" data-line-number="1008"></td>
        <td id="LC1008" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>list_of_ignored_locations</span> <span class=pl-c1>=</span> []</td>
      </tr>
      <tr>
        <td id="L1009" class="blob-num js-line-number" data-line-number="1009"></td>
        <td id="LC1009" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>with</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>UpdateCursor</span>(<span class=pl-s1>locations_fc</span>, [<span class=pl-s>&#39;location_id&#39;</span>, <span class=pl-s>&#39;ignore&#39;</span>], <span class=pl-s1>where_clause</span><span class=pl-c1>=</span><span class=pl-s1>query</span>) <span class=pl-k>as</span> <span class=pl-s1>ucursor</span>:</td>
      </tr>
      <tr>
        <td id="L1010" class="blob-num js-line-number" data-line-number="1010"></td>
        <td id="LC1010" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>for</span> <span class=pl-s1>row</span> <span class=pl-c1>in</span> <span class=pl-s1>ucursor</span>:</td>
      </tr>
      <tr>
        <td id="L1011" class="blob-num js-line-number" data-line-number="1011"></td>
        <td id="LC1011" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>list_of_ignored_locations</span>.<span class=pl-en>append</span>(<span class=pl-s1>row</span>[<span class=pl-c1>0</span>])</td>
      </tr>
      <tr>
        <td id="L1012" class="blob-num js-line-number" data-line-number="1012"></td>
        <td id="LC1012" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&#39;Location ID {} does not connect to the network and will be ignored&#39;</span>.<span class=pl-en>format</span>(<span class=pl-s1>row</span>[<span class=pl-c1>0</span>]))</td>
      </tr>
      <tr>
        <td id="L1013" class="blob-num js-line-number" data-line-number="1013"></td>
        <td id="LC1013" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>row</span>[<span class=pl-c1>1</span>] <span class=pl-c1>=</span> <span class=pl-c1>1</span></td>
      </tr>
      <tr>
        <td id="L1014" class="blob-num js-line-number" data-line-number="1014"></td>
        <td id="LC1014" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>ucursor</span>.<span class=pl-en>updateRow</span>(<span class=pl-s1>row</span>)</td>
      </tr>
      <tr>
        <td id="L1015" class="blob-num js-line-number" data-line-number="1015"></td>
        <td id="LC1015" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1016" class="blob-num js-line-number" data-line-number="1016"></td>
        <td id="LC1016" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-en>len</span>(<span class=pl-s1>list_of_ignored_locations</span>) <span class=pl-c1>&gt;</span> <span class=pl-c1>0</span>:</td>
      </tr>
      <tr>
        <td id="L1017" class="blob-num js-line-number" data-line-number="1017"></td>
        <td id="LC1017" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1018" class="blob-num js-line-number" data-line-number="1018"></td>
        <td id="LC1018" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>logger</span>.<span class=pl-en>result</span>(<span class=pl-s>&quot;# of locations not connected to network and ignored: <span class=pl-cce>\t</span>{}&quot;</span>.<span class=pl-en>format</span>(<span class=pl-en>len</span>(</td>
      </tr>
      <tr>
        <td id="L1019" class="blob-num js-line-number" data-line-number="1019"></td>
        <td id="LC1019" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>list_of_ignored_locations</span>)<span class=pl-c1>/</span><span class=pl-c1>2.0</span>))</td>
      </tr>
      <tr>
        <td id="L1020" class="blob-num js-line-number" data-line-number="1020"></td>
        <td id="LC1020" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>logger</span>.<span class=pl-en>info</span>(<span class=pl-s>&quot;note: check the log files for additional debug information.&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1021" class="blob-num js-line-number" data-line-number="1021"></td>
        <td id="LC1021" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1022" class="blob-num js-line-number" data-line-number="1022"></td>
        <td id="LC1022" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span>.<span class=pl-en>stopOperation</span>()</td>
      </tr>
      <tr>
        <td id="L1023" class="blob-num js-line-number" data-line-number="1023"></td>
        <td id="LC1023" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>edit</span>.<span class=pl-en>stopEditing</span>(<span class=pl-c1>True</span>)</td>
      </tr>
      <tr>
        <td id="L1024" class="blob-num js-line-number" data-line-number="1024"></td>
        <td id="LC1024" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1025" class="blob-num js-line-number" data-line-number="1025"></td>
        <td id="LC1025" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># flag locations which dont connect to the network in the DB</span></td>
      </tr>
      <tr>
        <td id="L1026" class="blob-num js-line-number" data-line-number="1026"></td>
        <td id="LC1026" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># -----------------------------------------------------------</span></td>
      </tr>
      <tr>
        <td id="L1027" class="blob-num js-line-number" data-line-number="1027"></td>
        <td id="LC1027" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>scenario_db</span> <span class=pl-c1>=</span> <span class=pl-s1>the_scenario</span>.<span class=pl-s1>main_db</span></td>
      </tr>
      <tr>
        <td id="L1028" class="blob-num js-line-number" data-line-number="1028"></td>
        <td id="LC1028" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1029" class="blob-num js-line-number" data-line-number="1029"></td>
        <td id="LC1029" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-s1>os</span>.<span class=pl-s1>path</span>.<span class=pl-en>exists</span>(<span class=pl-s1>scenario_db</span>):</td>
      </tr>
      <tr>
        <td id="L1030" class="blob-num js-line-number" data-line-number="1030"></td>
        <td id="LC1030" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1031" class="blob-num js-line-number" data-line-number="1031"></td>
        <td id="LC1031" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>with</span> <span class=pl-s1>sqlite3</span>.<span class=pl-en>connect</span>(<span class=pl-s1>scenario_db</span>) <span class=pl-k>as</span> <span class=pl-s1>db_con</span>:</td>
      </tr>
      <tr>
        <td id="L1032" class="blob-num js-line-number" data-line-number="1032"></td>
        <td id="LC1032" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1033" class="blob-num js-line-number" data-line-number="1033"></td>
        <td id="LC1033" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;connected to the db&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1034" class="blob-num js-line-number" data-line-number="1034"></td>
        <td id="LC1034" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1035" class="blob-num js-line-number" data-line-number="1035"></td>
        <td id="LC1035" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>db_cur</span> <span class=pl-c1>=</span> <span class=pl-s1>db_con</span>.<span class=pl-en>cursor</span>()</td>
      </tr>
      <tr>
        <td id="L1036" class="blob-num js-line-number" data-line-number="1036"></td>
        <td id="LC1036" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1037" class="blob-num js-line-number" data-line-number="1037"></td>
        <td id="LC1037" class="blob-code blob-code-inner js-file-line">            <span class=pl-c># iterate through the locations table and set ignore flag</span></td>
      </tr>
      <tr>
        <td id="L1038" class="blob-num js-line-number" data-line-number="1038"></td>
        <td id="LC1038" class="blob-code blob-code-inner js-file-line">            <span class=pl-c># if location_id is in the list_of_ignored_facilities</span></td>
      </tr>
      <tr>
        <td id="L1039" class="blob-num js-line-number" data-line-number="1039"></td>
        <td id="LC1039" class="blob-code blob-code-inner js-file-line">            <span class=pl-c># ------------------------------------------------------------</span></td>
      </tr>
      <tr>
        <td id="L1040" class="blob-num js-line-number" data-line-number="1040"></td>
        <td id="LC1040" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1041" class="blob-num js-line-number" data-line-number="1041"></td>
        <td id="LC1041" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;setting ignore fields for location in the DB locations and facilities tables&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1042" class="blob-num js-line-number" data-line-number="1042"></td>
        <td id="LC1042" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1043" class="blob-num js-line-number" data-line-number="1043"></td>
        <td id="LC1043" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>sql</span> <span class=pl-c1>=</span> <span class=pl-s>&quot;select location_id, ignore_location from locations;&quot;</span></td>
      </tr>
      <tr>
        <td id="L1044" class="blob-num js-line-number" data-line-number="1044"></td>
        <td id="LC1044" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1045" class="blob-num js-line-number" data-line-number="1045"></td>
        <td id="LC1045" class="blob-code blob-code-inner js-file-line">            <span class=pl-s1>db_cur</span>.<span class=pl-en>execute</span>(<span class=pl-s1>sql</span>)</td>
      </tr>
      <tr>
        <td id="L1046" class="blob-num js-line-number" data-line-number="1046"></td>
        <td id="LC1046" class="blob-code blob-code-inner js-file-line">            <span class=pl-k>for</span> <span class=pl-s1>row</span> <span class=pl-c1>in</span> <span class=pl-s1>db_cur</span>:</td>
      </tr>
      <tr>
        <td id="L1047" class="blob-num js-line-number" data-line-number="1047"></td>
        <td id="LC1047" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1048" class="blob-num js-line-number" data-line-number="1048"></td>
        <td id="LC1048" class="blob-code blob-code-inner js-file-line">                <span class=pl-k>if</span> <span class=pl-en>str</span>(<span class=pl-s1>row</span>[<span class=pl-c1>0</span>]) <span class=pl-c1>in</span> <span class=pl-s1>list_of_ignored_locations</span>:  <span class=pl-c># cast as string since location_id field is a string</span></td>
      </tr>
      <tr>
        <td id="L1049" class="blob-num js-line-number" data-line-number="1049"></td>
        <td id="LC1049" class="blob-code blob-code-inner js-file-line">                    <span class=pl-s1>ignore_flag</span> <span class=pl-c1>=</span> <span class=pl-s>&#39;network&#39;</span></td>
      </tr>
      <tr>
        <td id="L1050" class="blob-num js-line-number" data-line-number="1050"></td>
        <td id="LC1050" class="blob-code blob-code-inner js-file-line">                <span class=pl-k>else</span>:</td>
      </tr>
      <tr>
        <td id="L1051" class="blob-num js-line-number" data-line-number="1051"></td>
        <td id="LC1051" class="blob-code blob-code-inner js-file-line">                    <span class=pl-s1>ignore_flag</span> <span class=pl-c1>=</span> <span class=pl-s>&#39;false&#39;</span></td>
      </tr>
      <tr>
        <td id="L1052" class="blob-num js-line-number" data-line-number="1052"></td>
        <td id="LC1052" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>sql</span> <span class=pl-c1>=</span> <span class=pl-s>&quot;update locations set ignore_location = &#39;{}&#39; where location_id = &#39;{}&#39;;&quot;</span>.<span class=pl-en>format</span>(<span class=pl-s1>ignore_flag</span>,</td>
      </tr>
      <tr>
        <td id="L1053" class="blob-num js-line-number" data-line-number="1053"></td>
        <td id="LC1053" class="blob-code blob-code-inner js-file-line">                                                                                                     <span class=pl-s1>row</span>[<span class=pl-c1>0</span>])</td>
      </tr>
      <tr>
        <td id="L1054" class="blob-num js-line-number" data-line-number="1054"></td>
        <td id="LC1054" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>db_con</span>.<span class=pl-en>execute</span>(<span class=pl-s1>sql</span>)</td>
      </tr>
      <tr>
        <td id="L1055" class="blob-num js-line-number" data-line-number="1055"></td>
        <td id="LC1055" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1056" class="blob-num js-line-number" data-line-number="1056"></td>
        <td id="LC1056" class="blob-code blob-code-inner js-file-line">                <span class=pl-c># iterate through the facilities table,</span></td>
      </tr>
      <tr>
        <td id="L1057" class="blob-num js-line-number" data-line-number="1057"></td>
        <td id="LC1057" class="blob-code blob-code-inner js-file-line">                <span class=pl-c># and check if location_id is in the list_of_ignored_facilities</span></td>
      </tr>
      <tr>
        <td id="L1058" class="blob-num js-line-number" data-line-number="1058"></td>
        <td id="LC1058" class="blob-code blob-code-inner js-file-line">                <span class=pl-c># ------------------------------------------------------------</span></td>
      </tr>
      <tr>
        <td id="L1059" class="blob-num js-line-number" data-line-number="1059"></td>
        <td id="LC1059" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>sql</span> <span class=pl-c1>=</span> <span class=pl-s>&quot;update facilities set ignore_facility = &#39;{}&#39; where location_id = &#39;{}&#39;;&quot;</span>.<span class=pl-en>format</span>(<span class=pl-s1>ignore_flag</span>,</td>
      </tr>
      <tr>
        <td id="L1060" class="blob-num js-line-number" data-line-number="1060"></td>
        <td id="LC1060" class="blob-code blob-code-inner js-file-line">                                                                                                      <span class=pl-s1>row</span>[<span class=pl-c1>0</span>])</td>
      </tr>
      <tr>
        <td id="L1061" class="blob-num js-line-number" data-line-number="1061"></td>
        <td id="LC1061" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1062" class="blob-num js-line-number" data-line-number="1062"></td>
        <td id="LC1062" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>db_con</span>.<span class=pl-en>execute</span>(<span class=pl-s1>sql</span>)</td>
      </tr>
      <tr>
        <td id="L1063" class="blob-num js-line-number" data-line-number="1063"></td>
        <td id="LC1063" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1064" class="blob-num js-line-number" data-line-number="1064"></td>
        <td id="LC1064" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;finished: ignore_locations_not_connected_to_network&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1065" class="blob-num js-line-number" data-line-number="1065"></td>
        <td id="LC1065" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>return</span> <span class=pl-c1>1</span></td>
      </tr>
      <tr>
        <td id="L1066" class="blob-num js-line-number" data-line-number="1066"></td>
        <td id="LC1066" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1067" class="blob-num js-line-number" data-line-number="1067"></td>
        <td id="LC1067" class="blob-code blob-code-inner js-file-line"><span class=pl-c># ======================================================================================================================</span></td>
      </tr>
      <tr>
        <td id="L1068" class="blob-num js-line-number" data-line-number="1068"></td>
        <td id="LC1068" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1069" class="blob-num js-line-number" data-line-number="1069"></td>
        <td id="LC1069" class="blob-code blob-code-inner js-file-line"><span class=pl-c># this code subsets the road network so that only links within the minimum bounding geometry (MBG) buffer are</span></td>
      </tr>
      <tr>
        <td id="L1070" class="blob-num js-line-number" data-line-number="1070"></td>
        <td id="LC1070" class="blob-code blob-code-inner js-file-line"><span class=pl-c># preserved. This method ends with the arcpy.Compact_management() call that reduces the size of the geodatabase and</span></td>
      </tr>
      <tr>
        <td id="L1071" class="blob-num js-line-number" data-line-number="1071"></td>
        <td id="LC1071" class="blob-code blob-code-inner js-file-line"><span class=pl-c># increases overall performance when hooking facilities into the network, and exporting the road fc to a shapefile</span></td>
      </tr>
      <tr>
        <td id="L1072" class="blob-num js-line-number" data-line-number="1072"></td>
        <td id="LC1072" class="blob-code blob-code-inner js-file-line"><span class=pl-c># for networkX.</span></td>
      </tr>
      <tr>
        <td id="L1073" class="blob-num js-line-number" data-line-number="1073"></td>
        <td id="LC1073" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1074" class="blob-num js-line-number" data-line-number="1074"></td>
        <td id="LC1074" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1075" class="blob-num js-line-number" data-line-number="1075"></td>
        <td id="LC1075" class="blob-code blob-code-inner js-file-line"><span class=pl-k>def</span> <span class=pl-en>minimum_bounding_geometry</span>(<span class=pl-s1>the_scenario</span>, <span class=pl-s1>logger</span>):</td>
      </tr>
      <tr>
        <td id="L1076" class="blob-num js-line-number" data-line-number="1076"></td>
        <td id="LC1076" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>info</span>(<span class=pl-s>&quot;start: minimum_bounding_geometry&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1077" class="blob-num js-line-number" data-line-number="1077"></td>
        <td id="LC1077" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-s1>env</span>.<span class=pl-s1>workspace</span> <span class=pl-c1>=</span> <span class=pl-s1>the_scenario</span>.<span class=pl-s1>main_gdb</span></td>
      </tr>
      <tr>
        <td id="L1078" class="blob-num js-line-number" data-line-number="1078"></td>
        <td id="LC1078" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1079" class="blob-num js-line-number" data-line-number="1079"></td>
        <td id="LC1079" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># Clean up any left of layers from a previous run</span></td>
      </tr>
      <tr>
        <td id="L1080" class="blob-num js-line-number" data-line-number="1080"></td>
        <td id="LC1080" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>Exists</span>(<span class=pl-s>&quot;road_lyr&quot;</span>):</td>
      </tr>
      <tr>
        <td id="L1081" class="blob-num js-line-number" data-line-number="1081"></td>
        <td id="LC1081" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s>&quot;road_lyr&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1082" class="blob-num js-line-number" data-line-number="1082"></td>
        <td id="LC1082" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>Exists</span>(<span class=pl-s>&quot;rail_lyr&quot;</span>):</td>
      </tr>
      <tr>
        <td id="L1083" class="blob-num js-line-number" data-line-number="1083"></td>
        <td id="LC1083" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s>&quot;rail_lyr&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1084" class="blob-num js-line-number" data-line-number="1084"></td>
        <td id="LC1084" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>Exists</span>(<span class=pl-s>&quot;water_lyr&quot;</span>):</td>
      </tr>
      <tr>
        <td id="L1085" class="blob-num js-line-number" data-line-number="1085"></td>
        <td id="LC1085" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s>&quot;water_lyr&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1086" class="blob-num js-line-number" data-line-number="1086"></td>
        <td id="LC1086" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>Exists</span>(<span class=pl-s>&quot;pipeline_prod_trf_rts_lyr&quot;</span>):</td>
      </tr>
      <tr>
        <td id="L1087" class="blob-num js-line-number" data-line-number="1087"></td>
        <td id="LC1087" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s>&quot;pipeline_prod_trf_rts_lyr&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1088" class="blob-num js-line-number" data-line-number="1088"></td>
        <td id="LC1088" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>Exists</span>(<span class=pl-s>&quot;pipeline_crude_trf_rts_lyr&quot;</span>):</td>
      </tr>
      <tr>
        <td id="L1089" class="blob-num js-line-number" data-line-number="1089"></td>
        <td id="LC1089" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s>&quot;pipeline_crude_trf_rts_lyr&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1090" class="blob-num js-line-number" data-line-number="1090"></td>
        <td id="LC1090" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>Exists</span>(<span class=pl-s>&quot;Locations_MBG&quot;</span>):</td>
      </tr>
      <tr>
        <td id="L1091" class="blob-num js-line-number" data-line-number="1091"></td>
        <td id="LC1091" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s>&quot;Locations_MBG&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1092" class="blob-num js-line-number" data-line-number="1092"></td>
        <td id="LC1092" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>Exists</span>(<span class=pl-s>&quot;Locations_MBG_Buffered&quot;</span>):</td>
      </tr>
      <tr>
        <td id="L1093" class="blob-num js-line-number" data-line-number="1093"></td>
        <td id="LC1093" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s>&quot;Locations_MBG_Buffered&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1094" class="blob-num js-line-number" data-line-number="1094"></td>
        <td id="LC1094" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1095" class="blob-num js-line-number" data-line-number="1095"></td>
        <td id="LC1095" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># Determine the minimum bounding geometry of the scenario</span></td>
      </tr>
      <tr>
        <td id="L1096" class="blob-num js-line-number" data-line-number="1096"></td>
        <td id="LC1096" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># The advanced license is required to use the convex hull method. If not available, default to rectangle_by_area</span></td>
      </tr>
      <tr>
        <td id="L1097" class="blob-num js-line-number" data-line-number="1097"></td>
        <td id="LC1097" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># which will not subset things quite as small but is still better than no subsetting at all</span></td>
      </tr>
      <tr>
        <td id="L1098" class="blob-num js-line-number" data-line-number="1098"></td>
        <td id="LC1098" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>CheckProduct</span>(<span class=pl-s>&quot;ArcInfo&quot;</span>) <span class=pl-c1>==</span> <span class=pl-s>&quot;Available&quot;</span>:</td>
      </tr>
      <tr>
        <td id="L1099" class="blob-num js-line-number" data-line-number="1099"></td>
        <td id="LC1099" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>MinimumBoundingGeometry_management</span>(<span class=pl-s>&quot;Locations&quot;</span>, <span class=pl-s>&quot;Locations_MBG&quot;</span>, <span class=pl-s>&quot;CONVEX_HULL&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1100" class="blob-num js-line-number" data-line-number="1100"></td>
        <td id="LC1100" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1101" class="blob-num js-line-number" data-line-number="1101"></td>
        <td id="LC1101" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>else</span>:</td>
      </tr>
      <tr>
        <td id="L1102" class="blob-num js-line-number" data-line-number="1102"></td>
        <td id="LC1102" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>MinimumBoundingGeometry_management</span>(<span class=pl-s>&quot;Locations&quot;</span>, <span class=pl-s>&quot;Locations_MBG&quot;</span>, <span class=pl-s>&quot;RECTANGLE_BY_AREA&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1103" class="blob-num js-line-number" data-line-number="1103"></td>
        <td id="LC1103" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>logger</span>.<span class=pl-en>warning</span>(<span class=pl-s>&quot;The Advanced/ArcInfo license level of ArcGIS is not available. A slight modification to &quot;</span></td>
      </tr>
      <tr>
        <td id="L1104" class="blob-num js-line-number" data-line-number="1104"></td>
        <td id="LC1104" class="blob-code blob-code-inner js-file-line">                       <span class=pl-s>&quot;the minimum bounding geometry process is necessary to ensure FTOT can successfully run.&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1105" class="blob-num js-line-number" data-line-number="1105"></td>
        <td id="LC1105" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1106" class="blob-num js-line-number" data-line-number="1106"></td>
        <td id="LC1106" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># Buffer the minimum bounding geometry of the scenario</span></td>
      </tr>
      <tr>
        <td id="L1107" class="blob-num js-line-number" data-line-number="1107"></td>
        <td id="LC1107" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>Buffer_analysis</span>(<span class=pl-s>&quot;Locations_MBG&quot;</span>, <span class=pl-s>&quot;Locations_MBG_Buffered&quot;</span>, <span class=pl-s>&quot;100 Miles&quot;</span>, <span class=pl-s>&quot;FULL&quot;</span>, <span class=pl-s>&quot;ROUND&quot;</span>, <span class=pl-s>&quot;NONE&quot;</span>, <span class=pl-s>&quot;&quot;</span>,</td>
      </tr>
      <tr>
        <td id="L1108" class="blob-num js-line-number" data-line-number="1108"></td>
        <td id="LC1108" class="blob-code blob-code-inner js-file-line">                          <span class=pl-s>&quot;GEODESIC&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1109" class="blob-num js-line-number" data-line-number="1109"></td>
        <td id="LC1109" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1110" class="blob-num js-line-number" data-line-number="1110"></td>
        <td id="LC1110" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># Select the roads within the buffer</span></td>
      </tr>
      <tr>
        <td id="L1111" class="blob-num js-line-number" data-line-number="1111"></td>
        <td id="LC1111" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># -----------------------------------</span></td>
      </tr>
      <tr>
        <td id="L1112" class="blob-num js-line-number" data-line-number="1112"></td>
        <td id="LC1112" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>MakeFeatureLayer_management</span>(<span class=pl-s>&quot;road&quot;</span>, <span class=pl-s>&quot;road_lyr&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1113" class="blob-num js-line-number" data-line-number="1113"></td>
        <td id="LC1113" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>SelectLayerByLocation_management</span>(<span class=pl-s>&quot;road_lyr&quot;</span>, <span class=pl-s>&quot;INTERSECT&quot;</span>, <span class=pl-s>&quot;Locations_MBG_Buffered&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1114" class="blob-num js-line-number" data-line-number="1114"></td>
        <td id="LC1114" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1115" class="blob-num js-line-number" data-line-number="1115"></td>
        <td id="LC1115" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>result</span> <span class=pl-c1>=</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>GetCount_management</span>(<span class=pl-s>&quot;road&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1116" class="blob-num js-line-number" data-line-number="1116"></td>
        <td id="LC1116" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>count_all_roads</span> <span class=pl-c1>=</span> <span class=pl-en>float</span>(<span class=pl-s1>result</span>.<span class=pl-en>getOutput</span>(<span class=pl-c1>0</span>))</td>
      </tr>
      <tr>
        <td id="L1117" class="blob-num js-line-number" data-line-number="1117"></td>
        <td id="LC1117" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1118" class="blob-num js-line-number" data-line-number="1118"></td>
        <td id="LC1118" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>result</span> <span class=pl-c1>=</span> <span class=pl-s1>arcpy</span>.<span class=pl-v>GetCount_management</span>(<span class=pl-s>&quot;road_lyr&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1119" class="blob-num js-line-number" data-line-number="1119"></td>
        <td id="LC1119" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>count_roads_subset</span> <span class=pl-c1>=</span> <span class=pl-en>float</span>(<span class=pl-s1>result</span>.<span class=pl-en>getOutput</span>(<span class=pl-c1>0</span>))</td>
      </tr>
      <tr>
        <td id="L1120" class="blob-num js-line-number" data-line-number="1120"></td>
        <td id="LC1120" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1121" class="blob-num js-line-number" data-line-number="1121"></td>
        <td id="LC1121" class="blob-code blob-code-inner js-file-line">    <span class=pl-c>## CHANGE</span></td>
      </tr>
      <tr>
        <td id="L1122" class="blob-num js-line-number" data-line-number="1122"></td>
        <td id="LC1122" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-s1>count_all_roads</span> <span class=pl-c1>&gt;</span> <span class=pl-c1>0</span>:</td>
      </tr>
      <tr>
        <td id="L1123" class="blob-num js-line-number" data-line-number="1123"></td>
        <td id="LC1123" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>roads_percentage</span> <span class=pl-c1>=</span> <span class=pl-s1>count_roads_subset</span> <span class=pl-c1>/</span> <span class=pl-s1>count_all_roads</span></td>
      </tr>
      <tr>
        <td id="L1124" class="blob-num js-line-number" data-line-number="1124"></td>
        <td id="LC1124" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>else</span>:</td>
      </tr>
      <tr>
        <td id="L1125" class="blob-num js-line-number" data-line-number="1125"></td>
        <td id="LC1125" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>roads_percentage</span> <span class=pl-c1>=</span> <span class=pl-c1>0</span></td>
      </tr>
      <tr>
        <td id="L1126" class="blob-num js-line-number" data-line-number="1126"></td>
        <td id="LC1126" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1127" class="blob-num js-line-number" data-line-number="1127"></td>
        <td id="LC1127" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># Only subset if the subset will result in substantial reduction of the road network size</span></td>
      </tr>
      <tr>
        <td id="L1128" class="blob-num js-line-number" data-line-number="1128"></td>
        <td id="LC1128" class="blob-code blob-code-inner js-file-line">    <span class=pl-k>if</span> <span class=pl-s1>roads_percentage</span> <span class=pl-c1>&lt;</span> <span class=pl-c1>0.75</span>:</td>
      </tr>
      <tr>
        <td id="L1129" class="blob-num js-line-number" data-line-number="1129"></td>
        <td id="LC1129" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># Switch selection to identify what&#39;s outside the buffer</span></td>
      </tr>
      <tr>
        <td id="L1130" class="blob-num js-line-number" data-line-number="1130"></td>
        <td id="LC1130" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>SelectLayerByAttribute_management</span>(<span class=pl-s>&quot;road_lyr&quot;</span>, <span class=pl-s>&quot;SWITCH_SELECTION&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1131" class="blob-num js-line-number" data-line-number="1131"></td>
        <td id="LC1131" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1132" class="blob-num js-line-number" data-line-number="1132"></td>
        <td id="LC1132" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># Add in FC 1 roadways (going to keep all interstate highways)</span></td>
      </tr>
      <tr>
        <td id="L1133" class="blob-num js-line-number" data-line-number="1133"></td>
        <td id="LC1133" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>SelectLayerByAttribute_management</span>(<span class=pl-s>&quot;road_lyr&quot;</span>, <span class=pl-s>&quot;REMOVE_FROM_SELECTION&quot;</span>, <span class=pl-s>&quot;FCLASS = 1&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1134" class="blob-num js-line-number" data-line-number="1134"></td>
        <td id="LC1134" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1135" class="blob-num js-line-number" data-line-number="1135"></td>
        <td id="LC1135" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># Delete the features outside the buffer</span></td>
      </tr>
      <tr>
        <td id="L1136" class="blob-num js-line-number" data-line-number="1136"></td>
        <td id="LC1136" class="blob-code blob-code-inner js-file-line">        <span class=pl-k>with</span> <span class=pl-s1>arcpy</span>.<span class=pl-s1>da</span>.<span class=pl-v>UpdateCursor</span>(<span class=pl-s>&#39;road_lyr&#39;</span>, [<span class=pl-s>&#39;OBJECTID&#39;</span>]) <span class=pl-k>as</span> <span class=pl-s1>ucursor</span>:</td>
      </tr>
      <tr>
        <td id="L1137" class="blob-num js-line-number" data-line-number="1137"></td>
        <td id="LC1137" class="blob-code blob-code-inner js-file-line">            <span class=pl-k>for</span> <span class=pl-s1>ucursor_row</span> <span class=pl-c1>in</span> <span class=pl-s1>ucursor</span>:</td>
      </tr>
      <tr>
        <td id="L1138" class="blob-num js-line-number" data-line-number="1138"></td>
        <td id="LC1138" class="blob-code blob-code-inner js-file-line">                <span class=pl-s1>ucursor</span>.<span class=pl-en>deleteRow</span>()</td>
      </tr>
      <tr>
        <td id="L1139" class="blob-num js-line-number" data-line-number="1139"></td>
        <td id="LC1139" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1140" class="blob-num js-line-number" data-line-number="1140"></td>
        <td id="LC1140" class="blob-code blob-code-inner js-file-line">        <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s>&quot;road_lyr&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1141" class="blob-num js-line-number" data-line-number="1141"></td>
        <td id="LC1141" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1142" class="blob-num js-line-number" data-line-number="1142"></td>
        <td id="LC1142" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># # Select the rail within the buffer</span></td>
      </tr>
      <tr>
        <td id="L1143" class="blob-num js-line-number" data-line-number="1143"></td>
        <td id="LC1143" class="blob-code blob-code-inner js-file-line">        <span class=pl-c># # ---------------------------------</span></td>
      </tr>
      <tr>
        <td id="L1144" class="blob-num js-line-number" data-line-number="1144"></td>
        <td id="LC1144" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1145" class="blob-num js-line-number" data-line-number="1145"></td>
        <td id="LC1145" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s>&quot;Locations_MBG&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1146" class="blob-num js-line-number" data-line-number="1146"></td>
        <td id="LC1146" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>Delete_management</span>(<span class=pl-s>&quot;Locations_MBG_Buffered&quot;</span>)</td>
      </tr>
      <tr>
        <td id="L1147" class="blob-num js-line-number" data-line-number="1147"></td>
        <td id="LC1147" class="blob-code blob-code-inner js-file-line">
</td>
      </tr>
      <tr>
        <td id="L1148" class="blob-num js-line-number" data-line-number="1148"></td>
        <td id="LC1148" class="blob-code blob-code-inner js-file-line">    <span class=pl-c># finally, compact the geodatabase so the MBG has an effect on runtime.</span></td>
      </tr>
      <tr>
        <td id="L1149" class="blob-num js-line-number" data-line-number="1149"></td>
        <td id="LC1149" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>arcpy</span>.<span class=pl-v>Compact_management</span>(<span class=pl-s1>the_scenario</span>.<span class=pl-s1>main_gdb</span>)</td>
      </tr>
      <tr>
        <td id="L1150" class="blob-num js-line-number" data-line-number="1150"></td>
        <td id="LC1150" class="blob-code blob-code-inner js-file-line">    <span class=pl-s1>logger</span>.<span class=pl-en>debug</span>(<span class=pl-s>&quot;finish: minimum_bounding_geometry&quot;</span>)</td>
      </tr>
</table>

  <details class="details-reset details-overlay BlobToolbar position-absolute js-file-line-actions dropdown d-none" aria-hidden="true">
    <summary class="btn-octicon ml-0 px-2 p-0 bg-white border border-gray-dark rounded-1" aria-label="Inline file action toolbar">
      <svg class="octicon octicon-kebab-horizontal" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path d="M8 9a1.5 1.5 0 100-3 1.5 1.5 0 000 3zM1.5 9a1.5 1.5 0 100-3 1.5 1.5 0 000 3zm13 0a1.5 1.5 0 100-3 1.5 1.5 0 000 3z"></path></svg>
    </summary>
    <details-menu>
      <ul class="BlobToolbar-dropdown dropdown-menu dropdown-menu-se mt-2" style="width:185px">
        <li>
          <clipboard-copy role="menuitem" class="dropdown-item" id="js-copy-lines" style="cursor:pointer;">
            Copy lines
          </clipboard-copy>
        </li>
        <li>
          <clipboard-copy role="menuitem" class="dropdown-item" id="js-copy-permalink" style="cursor:pointer;">
            Copy permalink
          </clipboard-copy>
        </li>
        <li><a class="dropdown-item js-update-url-with-hash" id="js-view-git-blame" role="menuitem" href="/VolpeUSDOT/FTOT/blame/d9a19dc4879590346cf9ff899019f39a2f6dd14e/program/ftot_routing.py">View git blame</a></li>
          <li><a class="dropdown-item" id="js-new-issue" role="menuitem" href="/VolpeUSDOT/FTOT/issues/new">Reference in new issue</a></li>
      </ul>
    </details-menu>
  </details>

  </div>

    </div>

  


  <details class="details-reset details-overlay details-overlay-dark" id="jumpto-line-details-dialog">
    <summary data-hotkey="l" aria-label="Jump to line"></summary>
    <details-dialog class="Box Box--overlay d-flex flex-column anim-fade-in fast linejump" aria-label="Jump to line">
      <!-- '"` --><!-- </textarea></xmp> --></option></form><form class="js-jump-to-line-form Box-body d-flex" action="" accept-charset="UTF-8" method="get">
        <input class="form-control flex-auto mr-3 linejump-input js-jump-to-line-field" type="text" placeholder="Jump to line&hellip;" aria-label="Jump to line" autofocus>
        <button type="submit" class="btn" data-close-dialog>Go</button>
</form>    </details-dialog>
  </details>

    <div class="Popover anim-scale-in js-tagsearch-popover"
     hidden
     data-tagsearch-url="/VolpeUSDOT/FTOT/find-definition"
     data-tagsearch-ref="master"
     data-tagsearch-path="program/ftot_routing.py"
     data-tagsearch-lang="Python"
     data-hydro-click="{&quot;event_type&quot;:&quot;code_navigation.click_on_symbol&quot;,&quot;payload&quot;:{&quot;action&quot;:&quot;click_on_symbol&quot;,&quot;repository_id&quot;:162637563,&quot;ref&quot;:&quot;master&quot;,&quot;language&quot;:&quot;Python&quot;,&quot;originating_url&quot;:&quot;https://github.com/VolpeUSDOT/FTOT/blob/master/program/ftot_routing.py&quot;,&quot;user_id&quot;:45362680}}"
     data-hydro-click-hmac="7ecd0054e28b6ab2e492aa31e9063d896008b3778859ab017ee5db1e2fb2d985">
  <div class="Popover-message Popover-message--large Popover-message--top-left TagsearchPopover mt-1 mb-4 mx-auto Box box-shadow-large">
    <div class="TagsearchPopover-content js-tagsearch-popover-content overflow-auto" style="will-change:transform;">
    </div>
  </div>
</div>




  </div>
</div>

    </main>
  </div>

  </div>

        
<div class="footer container-xl width-full p-responsive" role="contentinfo">
    <div class="position-relative d-flex flex-row-reverse flex-lg-row flex-wrap flex-lg-nowrap flex-justify-center flex-lg-justify-between flex-sm-items-center pt-6 pb-2 mt-6 f6 text-gray border-top border-gray-light ">
      <a aria-label="Homepage" title="GitHub" class="footer-octicon d-none d-lg-block mr-lg-4" href="https://github.com">
        <svg height="24" class="octicon octicon-mark-github" viewBox="0 0 16 16" version="1.1" width="24" aria-hidden="true"><path fill-rule="evenodd" d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"></path></svg>
</a>
      <ul class="list-style-none d-flex flex-wrap col-12 flex-justify-center flex-lg-justify-between mb-2 mb-lg-0">
        <li class="mr-3 mr-lg-0">&copy; 2020 GitHub, Inc.</li>
          <li class="mr-3 mr-lg-0"><a data-ga-click="Footer, go to terms, text:terms" href="https://github.com/site/terms">Terms</a></li>
          <li class="mr-3 mr-lg-0"><a data-ga-click="Footer, go to privacy, text:privacy" href="https://github.com/site/privacy">Privacy</a></li>
            <li class="js-cookie-consent-preferences-link-container mr-3 mr-lg-0" hidden="hidden">
  <button data-ga-click="Footer, go to cookie preferences, text:cookie preferences" class="btn-link js-cookie-consent-preferences-link" type="button">Cookie Preferences</button>
</li>
          <li class="mr-3 mr-lg-0"><a data-ga-click="Footer, go to security, text:security" href="https://github.com/security">Security</a></li>
          <li class="mr-3 mr-lg-0"><a href="https://githubstatus.com/" data-ga-click="Footer, go to status, text:status">Status</a></li>
          <li class="mr-3 mr-lg-0"><a data-ga-click="Footer, go to help, text:help" href="https://docs.github.com">Help</a></li>
          <li class="mr-3 mr-lg-0"><a data-ga-click="Footer, go to contact, text:contact" href="https://github.com/contact">Contact GitHub</a></li>
          <li class="mr-3 mr-lg-0"><a href="https://github.com/pricing" data-ga-click="Footer, go to Pricing, text:Pricing">Pricing</a></li>
        <li class="mr-3 mr-lg-0"><a href="https://docs.github.com" data-ga-click="Footer, go to api, text:api">API</a></li>
        <li class="mr-3 mr-lg-0"><a href="https://services.github.com" data-ga-click="Footer, go to training, text:training">Training</a></li>
          <li class="mr-3 mr-lg-0"><a href="https://github.blog" data-ga-click="Footer, go to blog, text:blog">Blog</a></li>
          <li class="mr-3 mr-lg-0"><a data-ga-click="Footer, go to about, text:about" href="https://github.com/about">About</a></li>
      </ul>
    </div>
  <div class="d-flex flex-justify-center pb-6">
    <span class="f6 text-gray-light"></span>
  </div>
</div>



  <div id="ajax-error-message" class="ajax-error-message flash flash-error">
    <svg class="octicon octicon-alert" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M8.22 1.754a.25.25 0 00-.44 0L1.698 13.132a.25.25 0 00.22.368h12.164a.25.25 0 00.22-.368L8.22 1.754zm-1.763-.707c.659-1.234 2.427-1.234 3.086 0l6.082 11.378A1.75 1.75 0 0114.082 15H1.918a1.75 1.75 0 01-1.543-2.575L6.457 1.047zM9 11a1 1 0 11-2 0 1 1 0 012 0zm-.25-5.25a.75.75 0 00-1.5 0v2.5a.75.75 0 001.5 0v-2.5z"></path></svg>
    <button type="button" class="flash-close js-ajax-error-dismiss" aria-label="Dismiss error">
      <svg class="octicon octicon-x" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M3.72 3.72a.75.75 0 011.06 0L8 6.94l3.22-3.22a.75.75 0 111.06 1.06L9.06 8l3.22 3.22a.75.75 0 11-1.06 1.06L8 9.06l-3.22 3.22a.75.75 0 01-1.06-1.06L6.94 8 3.72 4.78a.75.75 0 010-1.06z"></path></svg>
    </button>
    You can’t perform that action at this time.
  </div>


    <script crossorigin="anonymous" async="async" integrity="sha512-O8GTqkyR57l/sxhU8bdDyhNYweSdc6TzDwJ7JvY4SJpZye2+L5ayXb5dDiHo45PLT+boMpfMts4kmDzwuv6ZBQ==" type="application/javascript" id="js-conditional-compat" data-src="https://github.githubassets.com/assets/compat-3bc193aa.js"></script>
    <script crossorigin="anonymous" integrity="sha512-qBeyRIjDR3DFKTE2D3j4NncHH72scmqyompBxHHrg7D5+rWW8q2xTxXbF7Z2g8Jx7UNpXfM+3R/FVU0SLgFrgw==" type="application/javascript" src="https://github.githubassets.com/assets/environment-a817b244.js"></script>
    <script crossorigin="anonymous" async="async" integrity="sha512-pDoSy+lGZEIqfmxmbGsQjCc9OnsB64vTSt1Qn5JZ4eYxqVgmookY2GSQGdaquPu/qTRix/kCkFDctCkqHpjYYA==" type="application/javascript" src="https://github.githubassets.com/assets/chunk-vendor-a43a12cb.js"></script>
    <script crossorigin="anonymous" async="async" integrity="sha512-BgbMlGbpOyAxA0hMwYcnj/fCd44k/o0dTbiYQEJqWlz9c6ppCR6/eSXWeACeR0FG48xRJGb8SvNZvYU7GofA6Q==" type="application/javascript" src="https://github.githubassets.com/assets/chunk-frameworks-0606cc94.js"></script>
    
    <script crossorigin="anonymous" async="async" integrity="sha512-5wJBDlN8x4rnahZLO1bueoTvkJDqds4RqBKCOm6jdn3AVyOVGuXC4ojnrbQbcza6H3y39Zdhy1ZzMYefQrkPCA==" type="application/javascript" src="https://github.githubassets.com/assets/behaviors-e702410e.js"></script>
    
      <script crossorigin="anonymous" async="async" integrity="sha512-54eoDRyRVUk3oS4N5a+zMKkRIRN8whZ3A1DVE/DS+vN7/wW7H9+5IueL/eISlx4hfljl/uGLW01BpUObUAJIGQ==" type="application/javascript" data-module-id="./chunk-contributions-spider-graph.js" data-src="https://github.githubassets.com/assets/chunk-contributions-spider-graph-e787a80d.js"></script>
      <script crossorigin="anonymous" async="async" integrity="sha512-Sjnsr0ANmUID/avvkPo/kS/uBlx9SuqXiR1HN8LrPMYWO7qdeurcIR2F5/hywcj7Zfd1tKVsHLsXZxKLCCH+aQ==" type="application/javascript" data-module-id="./chunk-drag-drop.js" data-src="https://github.githubassets.com/assets/chunk-drag-drop-4a39ecaf.js"></script>
      <script crossorigin="anonymous" async="async" integrity="sha512-hSAtuJpsIq3QiO5HtLmp0z+yDWhaHcVPWCxFDJBvKPHbUJ57nf6G71JDYFMN6tzbJR/5S8wJnvTDMBR2PnmrOA==" type="application/javascript" data-module-id="./chunk-jump-to.js" data-src="https://github.githubassets.com/assets/chunk-jump-to-85202db8.js"></script>
      <script crossorigin="anonymous" async="async" integrity="sha512-JSTirxBTeqnDkNzkska5W+unJU9VX7xePaQoER5lJr2++ojaXFE0of68/eBPKM+PCy2veP5SsVHYpnJuuXdPmA==" type="application/javascript" data-module-id="./chunk-profile-pins-element.js" data-src="https://github.githubassets.com/assets/chunk-profile-pins-element-2524e2af.js"></script>
      <script crossorigin="anonymous" async="async" integrity="sha512-Ry+8WCG9+PK9a/xLP7ZMueqNX2tfGZJAHo+38t9h95RdcD94SSHl8WpSiuMo0xSRE25ZO2Bw8ujHTLzaZhc87Q==" type="application/javascript" data-module-id="./chunk-randomColor.js" data-src="https://github.githubassets.com/assets/chunk-randomColor-472fbc58.js"></script>
      <script crossorigin="anonymous" async="async" integrity="sha512-kcVrUcNcBDAGY9PTKWZI8NnD+R/p3RvyLxlnBBzDXERoYGW20fL2EvY2gMes4MIbavUwb/4+HRAXLmUD3U4u9g==" type="application/javascript" data-module-id="./chunk-sortable-behavior.js" data-src="https://github.githubassets.com/assets/chunk-sortable-behavior-91c56b51.js"></script>
      <script crossorigin="anonymous" async="async" integrity="sha512-1j6a5EgjrMbQ66Ao5qIQwQ156E295uiL3SlH69NwrV57RR2ocVe+6kaNPvtdxzLdM9uDKv4fmR4oO4Oi3tLncQ==" type="application/javascript" data-module-id="./chunk-tweetsodium.js" data-src="https://github.githubassets.com/assets/chunk-tweetsodium-d63e9ae4.js"></script>
      <script crossorigin="anonymous" async="async" integrity="sha512-YuWmwhKSGCohhGVdKceBxjakFi7TBYMscav8X+KGTsbv2hThZUNX68JLZYgJwNKVU4bf+XUGhRm7gfqnkCVUqA==" type="application/javascript" data-module-id="./chunk-user-status-submit.js" data-src="https://github.githubassets.com/assets/chunk-user-status-submit-62e5a6c2.js"></script>
    
    <script crossorigin="anonymous" async="async" integrity="sha512-zvyRaVf+hIp05yTS028Gz5fnKK8NYbaoIaFBI/47EYCyHALTmSFJuel/1/5r/dEX29lTz4Nwpp9EQ3iRh4kPEQ==" type="application/javascript" src="https://github.githubassets.com/assets/repositories-cefc9169.js"></script>
<script crossorigin="anonymous" async="async" integrity="sha512-5AFAN1ydvGHphMD/LuiVufTeDdrVfeqJ9lEPKx9p2e65t4rSpPKMkSPilIcKBFuESiWkoOLcndYreqDbYCCcZg==" type="application/javascript" src="https://github.githubassets.com/assets/diffs-e4014037.js"></script>
<script crossorigin="anonymous" async="async" integrity="sha512-AAA2jGQdfutTTwRtgVUxmx8IWUG5ojtM6eL+lLn8oi+SMpmTSL5og4qdaARc+v2+sh9/OGOebjl3OHARm0LG9A==" type="application/javascript" src="https://github.githubassets.com/assets/github-0000368c.js"></script>
  <div class="js-stale-session-flash flash flash-warn flash-banner" hidden
    >
    <svg class="octicon octicon-alert" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M8.22 1.754a.25.25 0 00-.44 0L1.698 13.132a.25.25 0 00.22.368h12.164a.25.25 0 00.22-.368L8.22 1.754zm-1.763-.707c.659-1.234 2.427-1.234 3.086 0l6.082 11.378A1.75 1.75 0 0114.082 15H1.918a1.75 1.75 0 01-1.543-2.575L6.457 1.047zM9 11a1 1 0 11-2 0 1 1 0 012 0zm-.25-5.25a.75.75 0 00-1.5 0v2.5a.75.75 0 001.5 0v-2.5z"></path></svg>
    <span class="js-stale-session-flash-signed-in" hidden>You signed in with another tab or window. <a href="">Reload</a> to refresh your session.</span>
    <span class="js-stale-session-flash-signed-out" hidden>You signed out in another tab or window. <a href="">Reload</a> to refresh your session.</span>
  </div>
  <template id="site-details-dialog">
  <details class="details-reset details-overlay details-overlay-dark lh-default text-gray-dark hx_rsm" open>
    <summary role="button" aria-label="Close dialog"></summary>
    <details-dialog class="Box Box--overlay d-flex flex-column anim-fade-in fast hx_rsm-dialog hx_rsm-modal">
      <button class="Box-btn-octicon m-0 btn-octicon position-absolute right-0 top-0" type="button" aria-label="Close dialog" data-close-dialog>
        <svg class="octicon octicon-x" viewBox="0 0 16 16" version="1.1" width="16" height="16" aria-hidden="true"><path fill-rule="evenodd" d="M3.72 3.72a.75.75 0 011.06 0L8 6.94l3.22-3.22a.75.75 0 111.06 1.06L9.06 8l3.22 3.22a.75.75 0 11-1.06 1.06L8 9.06l-3.22 3.22a.75.75 0 01-1.06-1.06L6.94 8 3.72 4.78a.75.75 0 010-1.06z"></path></svg>
      </button>
      <div class="octocat-spinner my-6 js-details-dialog-spinner"></div>
    </details-dialog>
  </details>
</template>

  <div class="Popover js-hovercard-content position-absolute" style="display: none; outline: none;" tabindex="0">
  <div class="Popover-message Popover-message--bottom-left Popover-message--large Box box-shadow-large" style="width:360px;">
  </div>
</div>

    <div class="js-cookie-consent-banner" hidden>
  <div class="hx_cookie-banner p-2 p-sm-3 p-md-4">
    <div style="max-width: 1194px;" class="Box hx_cookie-banner-box box-shadow-medium mx-auto">
    <div class="Box-body border-0 py-0 px-3 px-md-4">
      <div class="js-main-cookie-banner hx_cookie-banner-main">
          <div class="d-md-flex flex-items-center py-3">
            <p class="f5 flex-1 mb-3 mb-md-0">
              
  We use <span class="text-bold">optional</span> third-party analytics cookies to understand how you use GitHub.com so we can build better products.

              <span class="btn-link js-cookie-consent-learn-more">Learn more</span>.
            </p>
            <div class="d-flex d-md-block flex-wrap flex-sm-nowrap">
              <button class="btn btn-outline flex-1 mr-1 mx-sm-1 m-md-0 ml-md-2 js-cookie-consent-accept">Accept</button>
              <button class="btn btn-outline flex-1 ml-1 m-md-0 ml-md-2 js-cookie-consent-reject">Reject</button>
            </div>
          </div>
        </div>

        <div class="js-cookie-details hx_cookie-banner-details" hidden>
          <div class="d-md-flex flex-items-center py-3">
            <p class="f5 flex-1 mb-2 mb-md-0">
              
  We use <span class="text-bold">optional</span> third-party analytics cookies to understand how you use GitHub.com so we can build better products.

              <br>
              You can always update your selection by clicking <span class="text-bold">Cookie Preferences</span> at the bottom of the page.
              For more information, see our <a href="https://docs.github.com/en/free-pro-team@latest/github/site-policy/github-privacy-statement">Privacy Statement</a>.
            </p>
          </div>

          <div class="d-md-flex flex-items-center py-3 border-top">
            <div class="f5 flex-1 mb-2 mb-md-0">
              <h5 class="mb-1">Essential cookies</h5>
              <p class="f6 mb-md-0">We use essential cookies to perform essential website functions, e.g. they're used to log you in. 
                <a href="https://docs.github.com/en/github/site-policy/github-subprocessors-and-cookies">Learn more</a>
              </p>
            </div>
            <div class="text-right">
              <h5 class="text-blue">Always active</h5>
            </div>
          </div>

          <div class="d-md-flex flex-items-center py-3 border-top">
            <div class="f5 flex-1 mb-2 mb-md-0">
              <h5 class="mb-1">Analytics cookies</h5>
              <p class="f6 mb-md-0">We use analytics cookies to understand how you use our websites so we can make them better, e.g. they're used to gather information about the pages you visit and how many clicks you need to accomplish a task. 
                <a href="https://docs.github.com/en/github/site-policy/github-subprocessors-and-cookies">Learn more</a>
              </p>
            </div>
            <div class="text-right">
              <div class="BtnGroup mt-1 mt-md-0 ml-2">
                <button class="btn btn-outline BtnGroup-item js-accept-analytics-cookies" type="button">Accept</button>
                <button class="btn btn-outline BtnGroup-item js-reject-analytics-cookies" type="button">Reject</button>
              </div>
            </div>
          </div>

          <div class="text-right py-3 border-top">
            <button class="btn btn-primary js-save-cookie-preferences" type="button" disabled>Save preferences</button>
          </div>
        </div>
</div></div>  </div>
</div>


  </body>
</html>


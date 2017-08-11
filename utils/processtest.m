function retvalue = processtest(n)
    rest = randi(10);
    disp(['Sleeping for ' char(rest) ' seconds']);
    pause(rest);
    retvalue = rest^2;
function [sup, inf] = ConfidenceIntervals(y,y_estimate, sigma_multiplier)

% calculate some statistics and variables
error = y-y_estimate;
relative_error = error./y_estimate;

%hyp: relative error is normally distributed N(mu,sigma)
mu = mean(relative_error);
sigma = std(relative_error);

%commonly used percentiles
%68,3% = P{ ? - 1,00 ? < X < ? + 1,00 ? }
%95,0% = P{ ? - 1,96 ? < X < ? + 1,96 ? }
%95,5% = P{ ? - 2,00 ? < X < ? + 2,00 ? }
%99,0% = P{ ? - 2,58 ? < X < ? + 2,58 ? }
%99,7% = P{ ? - 3,00 ? < X < ? + 3,00 ? }

%remove the average of the error if it is not 0 and multiply by the
%appropiate sigma
sup = y_estimate *(1 + mu + sigma_multiplier*sigma);
inf= y_estimate *(1 + mu - sigma_multiplier*sigma);

end

